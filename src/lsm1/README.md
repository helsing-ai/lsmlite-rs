File `lsm1-ae2e7fc.c` is the amalgamated version of all source files found at `https://github.com/sqlite/sqlite/tree/master/ext/lsm1` (except for `lsm_vtab.c` due to the inclusion of `sqlite3ext.h`). Suffix `ae2e7fc` is the short hash of the last official commit done to `lsm1`'s directory as of March 27th 2023. 

To locally update `lsm1`'s code using the latest version contained in SQLite3's source code, run `tclsh tool/mklsm1c.tcl` inside SQLite3's directory `ext/lsm1` (after having commented out the line containing `%dir%/lsm_vtab.c` in `tool/mklsm1c.tcl`). This will produce the amalgamated file `lsm1.c` that can be then used in `build.rs` - and thus used by our bindings upon new compilation.

The following patch still needs to be applied, as the lsm1 build originally
depends on a `# define fdatasync fsync`, which is done in [another
file](https://github.com/sqlite/sqlite/blob/master/src/os_unix.c#L3595) in the
sqlite build.

```diff
diff --git a/src/lsm1/lsm1-ae2e7fc.c b/src/lsm1/lsm1-ae2e7fc.c
index 9088559..b58bc33 100644
--- a/src/lsm1/lsm1-ae2e7fc.c
+++ b/src/lsm1/lsm1-ae2e7fc.c
@@ -19606,7 +19606,7 @@ static int lsmPosixOsSync(lsm_file *pFile){
   if( p->pMap ){
     prc = msync(p->pMap, p->nMap, MS_SYNC);
   }
-  if( prc==0 ) prc = fdatasync(p->fd);
+  if( prc==0 ) prc = fsync(p->fd);
   if( prc<0 ) rc = LSM_IOERR_BKPT;
 #else
   (void)pFile;
```
