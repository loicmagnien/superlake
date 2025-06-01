import os
import sys
import ast
from collections import defaultdict, deque
import importlib
import threading


class SuperOrchestrator:
    def __init__(
        self, super_spark, catalog_name, logger, managed, superlake_dt, super_tracer, environment,
        project_root, ingestion_folder='ingestion', modelisation_folder='modelisation',
    ):
        self.super_spark = super_spark
        self.catalog_name = catalog_name
        self.logger = logger
        self.managed = managed
        self.superlake_dt = superlake_dt
        self.super_tracer = super_tracer
        self.environment = environment
        self.project_root = project_root
        self.ingestion_folder = ingestion_folder
        self.modelisation_folder = modelisation_folder

        # get the basename of the project root
        self.module_root = os.path.basename(project_root)

        # Add the project root to sys.path
        module_root_idx = project_root.rfind(f'{self.module_root}')
        if module_root_idx != -1:
            parent_dir = project_root[:module_root_idx].rstrip(os.sep)
            if parent_dir and parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)
        elif project_root not in sys.path:
            sys.path.insert(0, project_root)

        # Directories to scan
        self.BASE_DIRS = [os.path.join(project_root, ingestion_folder), os.path.join(project_root, modelisation_folder)]
        self.logger.info(f"base directories: ingestion: {self.BASE_DIRS[0]}, modelisation: {self.BASE_DIRS[1]}")

    def collect_dependencies(self, graph, target_file):
        """
        Given a dependency graph and a target file (basename), return a set of all files
        (including the target) that are dependencies (recursively) of the target.
        """
        deps = set()

        def visit(node):
            if node in deps:
                return
            deps.add(node)
            for dep in graph[node]:
                visit(dep)
        visit(target_file)
        return deps

    def collect_dependents(self, graph, target_file):
        """
        Given a dependency graph and a target file (basename), return a set of all files
        (including the target) that are dependents (recursively) of the target.
        """
        # Build reverse graph: for each node, who depends on it
        reverse_graph = {k: [] for k in graph}
        for node, deps in graph.items():
            for dep in deps:
                reverse_graph[dep].append(node)
        dependents = set()

        def visit(node):
            if node in dependents:
                return
            dependents.add(node)
            for dep in reverse_graph[node]:
                visit(dep)
        visit(target_file)
        return dependents

    def discover_files(self, base_dirs):
        py_files = []
        for base in base_dirs:
            for root, _, files in os.walk(base):
                # Skip folders starting with '__'
                if any(part.startswith('__') for part in os.path.relpath(root, base).split(os.sep) if part):
                    continue
                for fname in files:
                    if fname.endswith('.py') and not fname.startswith('__'):
                        py_files.append(os.path.join(root, fname))
        return py_files

    def parse_dependencies(self, py_file):
        with open(py_file, 'r') as f:
            tree = ast.parse(f.read(), filename=py_file)
        deps = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.module and (
                    node.module.startswith(f'{self.module_root}.{self.ingestion_folder}') or
                    node.module.startswith(f'{self.module_root}.{self.modelisation_folder}')
                ):
                    dep = node.module.split('.')[-1] + '.py'
                    deps.append(dep)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if (
                        alias.name.startswith(f'{self.module_root}.{self.ingestion_folder}') or
                        alias.name.startswith(f'{self.module_root}.{self.modelisation_folder}')
                    ):
                        dep = alias.name.split('.')[-1] + '.py'
                        deps.append(dep)
        return deps

    def build_graph(self, py_files):
        graph = defaultdict(list)
        name_map = {os.path.basename(f): f for f in py_files}
        for f in py_files:
            deps = self.parse_dependencies(f)
            for dep in deps:
                if dep in name_map:
                    graph[os.path.basename(f)].append(dep)
        # Ensure all files are nodes
        for f in py_files:
            graph.setdefault(os.path.basename(f), [])
        return graph, name_map

    def topo_sort(self, graph):
        indegree = defaultdict(int)
        for node in graph:
            for dep in graph[node]:
                indegree[dep] += 1
        queue = deque([n for n in graph if indegree[n] == 0])
        result = []
        visited = set()
        while queue:
            node = queue.popleft()
            if node in visited:
                continue
            visited.add(node)
            result.append(node)
            for dep in graph[node]:
                indegree[dep] -= 1
                if indegree[dep] == 0:
                    queue.append(dep)
        # Add any isolated nodes
        all_nodes = set(graph.keys()) | {d for deps in graph.values() for d in deps}
        for n in all_nodes:
            if n not in result:
                result.append(n)
        # Cycle detection
        if len(result) < len(all_nodes):
            cycle_nodes = all_nodes - set(result)
            raise RuntimeError(f"Cyclic dependency detected among: {', '.join(cycle_nodes)}")
        return result[::-1]

    def find_cycle(self, graph):
        """
        Returns a list of all cycles in the graph. Each cycle is represented as a list of node names.
        If no cycles are found, returns an empty list.
        """
        visited = set()
        stack = []
        cycles = set()

        def dfs(node):
            if node in stack:
                # Found a cycle, extract the cycle path
                idx = stack.index(node)
                cycle = tuple(stack[idx:] + [node])
                # Normalize cycle to avoid duplicates (start from min node)
                min_idx = min(range(len(cycle)), key=lambda i: cycle[i])
                normalized = cycle[min_idx:] + cycle[:min_idx]
                cycles.add(normalized)
                return
            if node in visited:
                return
            visited.add(node)
            stack.append(node)
            for neighbor in graph[node]:
                dfs(neighbor)
            stack.pop()

        for node in graph:
            if node not in visited:
                dfs(node)
        # Convert cycles from tuples to lists for output
        return [list(c) for c in cycles]

    def get_module_and_func(self, file_path):
        parts = os.path.normpath(file_path).split(os.sep)
        base = os.path.splitext(os.path.basename(file_path))[0]
        if "ingestion" in parts:
            func_name = f"get_pipeline_objects_{base}"
            folder = "ingestion"
        elif "modelisation" in parts:
            func_name = f"get_model_{base}"
            folder = "modelisation"
        else:
            raise ValueError(f"Could not determine pipeline type for path: {file_path}")
        # Find the module name from the project root
        if f'{self.module_root}' in parts:
            idx = len(parts) - 1 - parts[::-1].index(f'{self.module_root}')
            module_name = '.'.join(parts[idx:]).replace('.py', '')
        else:
            raise ValueError(f"Could not find '{self.module_root}' in path: {file_path}")
        return module_name, func_name, folder

    def import_and_run(self, module_name, func_name, folder, params):
        mod = importlib.import_module(module_name)
        func = getattr(mod, func_name)
        if folder == "ingestion":
            (super_spark, _, logger, _, superlake_dt, super_tracer, environment) = params
            (bronze_table, silver_table, cdc_function, tra_function, del_function) = func(*params[:5])
            from superlake.core import SuperPipeline
            pipeline = SuperPipeline(
                logger=logger,
                super_spark=super_spark,
                super_tracer=super_tracer,
                superlake_dt=superlake_dt,
                pipeline_name=module_name.split('.')[-1],
                bronze_table=bronze_table,
                silver_table=silver_table,
                cdc_function=cdc_function,
                tra_function=tra_function,
                del_function=del_function,
                force_cdc=False,
                force_caching=True,
                environment=environment
            )
            pipeline.execute()
        else:
            (super_spark, _, logger, _, superlake_dt, super_tracer, environment) = params
            table, generator = func(*params[:5])
            from superlake.core import SuperSimplePipeline

            def wrapped_model_function(_super_spark, _superlake_dt):
                return generator(*params[:5])
            pipeline = SuperSimplePipeline(
                logger=logger,
                super_spark=super_spark,
                super_tracer=super_tracer,
                superlake_dt=superlake_dt,
                pipeline_name=module_name.split('.')[-1],
                function=wrapped_model_function,
                table=table,
                environment=environment
            )
            pipeline.execute()

    def get_parallel_groups(self, graph, mode='process_first'):
        if mode == 'process_first':
            # group nodes by dependency level (longest path from any root)
            from collections import defaultdict, deque
            levels = {}

            def compute_level(node):
                if node in levels:
                    return levels[node]
                if not graph[node]:
                    levels[node] = 0
                    return 0
                max_dep_level = 0
                for dep in graph[node]:
                    dep_level = compute_level(dep) + 1
                    if dep_level > max_dep_level:
                        max_dep_level = dep_level
                levels[node] = max_dep_level
                return max_dep_level
            for node in graph:
                compute_level(node)
            level_groups = defaultdict(list)
            for node, lvl in levels.items():
                level_groups[lvl].append(node)
            return [level_groups[lvl] for lvl in sorted(level_groups.keys())]
        elif mode == 'process_last':
            # Kahn's algorithm for level-order topological sort
            from collections import defaultdict, deque
            indegree = defaultdict(int)
            for node in graph:
                for dep in graph[node]:
                    indegree[dep] += 1
            zero_indegree = deque([n for n in graph if indegree[n] == 0])
            visited = set()
            groups = []
            while zero_indegree:
                current_group = []
                next_zero_indegree = deque()
                while zero_indegree:
                    node = zero_indegree.popleft()
                    if node in visited:
                        continue
                    visited.add(node)
                    current_group.append(node)
                    for dep in graph[node]:
                        indegree[dep] -= 1
                        if indegree[dep] == 0:
                            next_zero_indegree.append(dep)
                if current_group:
                    groups.append(current_group)
                zero_indegree = next_zero_indegree
            return groups
        else:
            raise ValueError(f"Invalid mode: {mode}")

    def orchestrate(
            self,
            loading_mode='file',
            orchestration_mode='process_last',
            target_pipelines=None,
            direction='all',
            parallelize_groups=False,
            fail_fast=True,
            skip_downstream_on_failure=False
            ):
        """
        Orchestrate the execution of data pipelines with dependency management, parallelization, and robust error handling.

        Parameters:
            loading_mode (str):
                How to discover and load pipeline files. Typical value: 'file'.
            orchestration_mode (str):
                The order in which pipeline groups are processed.
                - 'process_first': Roots to leaves (upstream to downstream).
                - 'process_last': Leaves to roots (downstream to upstream).
            target_pipelines (list of str):
                List of pipeline filenames (e.g., ['velib_station_info.py']) to use as starting points for orchestration.
            direction (str):
                Which part of the dependency graph to process relative to target_pipelines:
                - 'upstream': Only dependencies of the targets (and the targets themselves).
                - 'downstream': Only dependents of the targets (and the targets themselves).
                - 'all': Both upstream and downstream pipelines (full subgraph).
                - 'none': Only the specified target_pipelines, with no dependencies.
            parallelize_groups (bool):
                If True, pipelines within each group are run in parallel threads. If False, run serially.
            fail_fast (bool):
                If True, stop execution as soon as any pipeline fails. If False, log errors and continue.
            skip_downstream_on_failure (bool):
                If True, a pipeline will be skipped if all of its upstream dependencies (from the full dependency graph) have failed or been skipped.
                If False, downstream pipelines are always run.

        Features:
            - Dependency graph analysis: Automatically discovers dependencies between pipeline files.
            - Cycle detection: Detects and logs cyclic dependencies in the pipeline graph.
            - Group-based orchestration: Pipelines are grouped by dependency level and processed in order.
            - Thread-safe status tracking: Safely tracks the status (success, failed, skipped) of each pipeline.
            - Contextual logging: Logs show which pipeline or orchestrator step is being executed.
            - Partial graph execution: Orchestrate only a subset of the full pipeline graph by specifying target_pipelines and direction.
            - Cascading skips: If a pipeline is skipped due to all upstreams failing/skipped, its downstreams will also be skipped in cascade.
        """
        if target_pipelines is None:
            target_pipelines = []
        with self.logger.sub_name_context('SuperOrchestrator'):
            # managing dependencies through files
            self.logger.info(f"Orchestrating pipelines in {loading_mode} mode with {orchestration_mode} orchestration mode")
            if loading_mode == 'file':
                py_files = self.discover_files(self.BASE_DIRS)
                graph, name_map = self.build_graph(py_files)

                # Save the full dependency graph before filtering
                full_graph = graph.copy()

                # If target_pipelines are specified, filter the graph
                if target_pipelines:
                    # Ensure all are .py and exist
                    targets = []
                    for tp in target_pipelines:
                        if not tp.endswith('.py'):
                            tp += '.py'
                        if tp not in graph:
                            raise ValueError(f"Target pipeline {tp} not found in discovered files.")
                        targets.append(tp)

                    if direction == 'none':
                        graph = {tp: [] for tp in targets}
                        name_map = {tp: name_map[tp] for tp in targets}
                    else:
                        if direction == 'upstream':
                            deps = set()
                            for tp in targets:
                                deps |= self.collect_dependencies(graph, tp)
                        elif direction == 'downstream':
                            deps = set()
                            for tp in targets:
                                deps |= self.collect_dependents(graph, tp)
                        elif direction == 'all':
                            deps = set()
                            for tp in targets:
                                deps |= self.collect_dependencies(graph, tp)
                                deps |= self.collect_dependents(graph, tp)
                        else:
                            raise ValueError("direction must be 'upstream', 'downstream', 'all', or 'none'")
                        # Filter graph and name_map to only include these files
                        graph = {k: [d for d in v if d in deps] for k, v in graph.items() if k in deps}
                        name_map = {k: v for k, v in name_map.items() if k in deps}

                # set the params
                params = (
                    self.super_spark,
                    self.catalog_name,
                    self.logger,
                    self.managed,
                    self.superlake_dt,
                    self.super_tracer,
                    self.environment
                )
                cycles = self.find_cycle(graph)
                if cycles:
                    self.logger.error("Cyclic dependency detected in the graph!")
                    self.logger.error(cycles)
                else:
                    try:
                        parallel_groups_list = self.get_parallel_groups(graph, mode=orchestration_mode)
                        if orchestration_mode == 'process_first':
                            groups_in_order = parallel_groups_list
                        elif orchestration_mode == 'process_last':
                            groups_in_order = list(reversed(parallel_groups_list))
                        else:
                            raise ValueError(f"Invalid orchestration mode: {orchestration_mode}")
                        # display the orchestration plan in a readable format
                        print("-------------------------------  SuperOrchestrator -------------------------------")
                        print("\nbase directories:")
                        for base_dir in self.BASE_DIRS:
                            print(f"  - {base_dir}")
                        print("\ndiscovered files:")
                        for file in py_files:
                            # Show only the path from the project_root basefolder (self.module_root)
                            parts = os.path.normpath(file).split(os.sep)
                            if self.module_root in parts:
                                idx = len(parts) - 1 - parts[::-1].index(self.module_root)
                                display_path = os.path.join(*parts[idx:])
                            else:
                                display_path = file
                            print(f"  - {display_path}")
                        print("\nParameters:")
                        print(f" - loading mode: {loading_mode}")
                        print(f" - target pipelines: {target_pipelines}")
                        print(f" - orchestration mode: {orchestration_mode}")
                        print(f" - direction: {direction}")
                        print(f" - parallelize groups: {parallelize_groups}")
                        print(f" - fail fast: {fail_fast}")
                        print(f" - skip downstream on failure: {skip_downstream_on_failure}")
                        print("\nOrchestration plan:")
                        for i, group in enumerate(groups_in_order, 1):
                            rel_paths = [os.path.relpath(name_map[f], os.path.commonpath(self.BASE_DIRS)) for f in group]
                            print(f"  Group {i}:")
                            for rel_path in rel_paths:
                                print(f"    - {rel_path}")
                        print("\n--------------------------------------------------------------------------------\n")
                        # Build reverse graph for upstream lookup (filtered graph)
                        reverse_graph = {k: [] for k in graph}
                        for node, deps in graph.items():
                            for dep in deps:
                                reverse_graph[dep].append(node)
                        # Track pipeline status
                        pipeline_status = {}
                        pipeline_status_lock = threading.Lock()
                        # run the orchestration plan
                        for i, group in enumerate(groups_in_order, 1):
                            rel_paths = [os.path.relpath(name_map[f], os.path.commonpath(self.BASE_DIRS)) for f in group]
                            self.logger.info(f"Processing orchestration group {i}")
                            self.logger.info(f"files to run: {rel_paths}")

                            def should_skip_pipeline(fname):
                                if not skip_downstream_on_failure:
                                    return False
                                # Use full_graph to get all true upstreams (dependencies of fname)
                                all_upstreams = full_graph.get(fname, [])
                                if not all_upstreams:
                                    return False
                                with pipeline_status_lock:
                                    return all(
                                        (up not in pipeline_status and up not in graph) or pipeline_status.get(up) in ('failed', 'skipped')
                                        for up in all_upstreams
                                    )

                            def run_pipeline(fname, error_list=None):
                                with self.logger.sub_name_context('SuperOrchestrator'):
                                    if should_skip_pipeline(fname):
                                        self.logger.info(f"Skipping {fname} because all upstream dependencies failed or were skipped.")
                                        with pipeline_status_lock:
                                            pipeline_status[fname] = 'skipped'
                                        return
                                    try:
                                        self.logger.info(f"Running {fname}...")
                                        file_path = name_map[fname]
                                        module_name, func_name, folder = self.get_module_and_func(file_path)
                                        self.logger.info(f"Running {module_name}.{func_name} as a {folder} pipeline...")
                                        self.import_and_run(module_name, func_name, folder, params)
                                        with pipeline_status_lock:
                                            pipeline_status[fname] = 'success'
                                    except Exception as e:
                                        self.logger.error(f"Pipeline {fname} failed: {e}", exc_info=True)
                                        with pipeline_status_lock:
                                            pipeline_status[fname] = 'failed'
                                        if error_list is not None:
                                            error_list.append(e)
                                        if fail_fast:
                                            raise

                            if parallelize_groups:
                                threads = []
                                errors = []
                                errors_lock = threading.Lock()
                                fail_event = threading.Event()

                                def thread_run(fname):
                                    if fail_event.is_set():
                                        return
                                    try:
                                        run_pipeline(fname, error_list=errors)
                                    except Exception as e:
                                        with errors_lock:
                                            errors.append(e)
                                        fail_event.set()

                                for fname in group:
                                    t = threading.Thread(target=thread_run, args=(fname,))
                                    t.start()
                                    threads.append(t)
                                for t in threads:
                                    t.join()
                                if fail_fast and errors:
                                    raise errors[0]
                            else:
                                for fname in group:
                                    run_pipeline(fname)
                    except RuntimeError as e:
                        self.logger.error(f"Orchestration error: {str(e)}")
            else:
                raise ValueError(f"Invalid loading mode: {loading_mode}")
