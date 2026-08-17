# Locate the MySQL / MariaDB client library.
# Sets MySQL_FOUND, MySQL_INCLUDE_DIR, MySQL_LIBRARIES.

find_path(MySQL_INCLUDE_DIR
        NAMES mysql.h
        HINTS $ENV{MYSQL_DIR} $ENV{MYSQL_ROOT}
        PATH_SUFFIXES mysql mariadb include include/mysql include/mariadb
        PATHS /usr /usr/local /opt /opt/local
)

find_library(MySQL_LIBRARY
        NAMES mysqlclient mariadb libmysqlclient libmariadb
        HINTS $ENV{MYSQL_DIR} $ENV{MYSQL_ROOT}
        PATH_SUFFIXES lib lib64
        PATHS /usr /usr/local /opt /opt/local
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(MySQL DEFAULT_MSG MySQL_LIBRARY MySQL_INCLUDE_DIR)

if (MySQL_FOUND)
    set(MySQL_LIBRARIES ${MySQL_LIBRARY})
endif ()

mark_as_advanced(MySQL_INCLUDE_DIR MySQL_LIBRARY)
