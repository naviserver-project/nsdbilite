#
# nsdbilite configuration example.
#
#     The nsdbilite SQLite database driver takes two
#     extra configuration parmeters: datasource and sqlitebusyretries.
#
#     datasource: a path in the filesystem, or the special token :memory:
#     sqlitebusyretries: default 100
#


#
# Global pools.
#
ns_section "ns/modules"
ns_param   pool1          $bindir/nsdbilite.so


#
# Private pools
#
ns_section "ns/server/server1/modules"
ns_param   pool2          $bindir/nsdbilite.so


#
# Pool 2 configuration.
#
ns_section "ns/server/server1/module/pool2"
#
# The following are standard nsdbi config options.
# See nsdbi for details.
#
ns_param   default        true ;# This is the default pool for server1.
ns_param   handles        0    ;# Max open handles to db, 0 for per-thread
ns_param   maxwiat        10   ;# Seconds to wait if handle unavailable.
ns_param   maxidle        0    ;# Handle closed after maxidle seconds if unused.
ns_param   maxopen        0    ;# Handle closed after maxopen seconds, regardles of use.
ns_param   maxqueries     0    ;# Handle closed after maxqueries sql queries.
ns_param   checkinterval  600  ;# Check for idle handles every 10 minutes.
#
# Following is the sqlite connection info that specifies
# which database file to connect to.
#
ns_param   datasource     ":memory:"
#ns_param   sqlitebusyretries     100
