# How To Run The Profiler For Loading Relations

For interactive use, loading all relations is a speed bump.
Here are steps to turn on profiling so that we can reduce the
time to load all relations.

```diff
diff --git a/python/etl/relation.py b/python/etl/relation.py
index 7c6c844..2172522 100644
--- a/python/etl/relation.py
+++ b/python/etl/relation.py
@@ -144,6 +144,10 @@ class RelationDescription:
     @staticmethod
     def load_in_parallel(relations: List["RelationDescription"]) -> None:
         """Load all relations' table design file in parallel."""
+        for relation in relations:
+            relation.load()
+        return
+
         with etl.timer.Timer() as timer:
             # TODO With Python 3.6, we should pass in a thread_name_prefix
             with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
@@ -508,7 +512,19 @@ def order_by_dependencies(relation_descriptions: List[RelationDescription]) -> L
     Side-effect: the order and level of the relations is set.
     (So should this be invoked a second time, we'll skip computations if order is already set.)
     """
-    RelationDescription.load_in_parallel(relation_descriptions)
+    import cProfile
+    import pstats
+    from pstats import SortKey
+
+    p = cProfile.Profile()
+    p.runcall(RelationDescription.load_in_parallel, relation_descriptions)
+    p.dump_stats('restats')
+
+    p = pstats.Stats('restats')
+    p.sort_stats(SortKey.CUMULATIVE).print_stats(10)
+    p.sort_stats(SortKey.TIME).print_stats(10)
+
+    # RelationDescription.load_in_parallel(relation_descriptions)
 
     # Sorting is all-or-nothing so we get away with checking for just one relation's order.
     if relation_descriptions and relation_descriptions[0]._execution_order is not None:
```
