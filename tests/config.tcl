#
# sqlite test config.
#


set homedir   [pwd]
set bindir    [file dirname [ns_info nsd]]



#
# Global AOLserver parameters.
#

ns_section "ns/parameters"
ns_param   home           $homedir
ns_param   tcllibrary     $bindir/../tcl
ns_param   logdebug       false

ns_section "ns/modules"
ns_param   global1        $homedir/nsdbilite.so
ns_param   global2        $homedir/nsdbilite.so

ns_section "ns/module/nssock/servers"
ns_param   server1         server1

ns_section "ns/servers"
ns_param   server1         "Server One"


#
# Server One configuration.
#

ns_section "ns/server/server1/tcl"
ns_param   initfile        ${bindir}/init.tcl
#ns_param   library         $homedir/tests/testserver/modules

ns_section "ns/server/server1/modules"
ns_param   pool1           $homedir/nsdbilite.so

#
# Database configuration.
#

ns_section "ns/module/global1"
ns_param   maxhandles      2

ns_section "ns/module/global2"
ns_param   maxhandles      2

ns_section "ns/server/server1/module/pool1"
ns_param   datasource      :memory:    ;# in-memory database
ns_param   default         true
ns_param   maxhandles      5
ns_param   maxidle         20          ;# Handle closed after maxidle seconds if unused.
ns_param   maxopen         40          ;# Handle closed after maxopen seconds, regardles of use.
ns_param   maxqueries      100000      ;# Handle closed after maxqueries sql queries.
ns_param   checkinterval   30          ;# Check for stale handles every 15 seconds.

