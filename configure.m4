#
# Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
# See file LICENSE for terms.
#

     #
     # Enable fault-tolerance
     #
     AC_ARG_ENABLE([fault-tolerance],
         AS_HELP_STRING([--enable-fault-tolerance],
                        [Enable fault-tolerance, default: NO]),
         [],
         [enable_fault_tolerance=no])
     AS_IF([test "x$enable_fault_tolerance" = xyes],
        [AS_MESSAGE([enabling with fault-tolerance])
         AC_DEFINE([ENABLE_FAULT_TOLERANCE], [1], [Enable fault-tolerance])],
        [:]
       )


ucg_modules=":builtin"
m4_include([src/ucg/base/configure.m4])
m4_include([src/ucg/builtin/configure.m4])
AC_DEFINE_UNQUOTED([ucg_MODULES], ["${ucg_modules}"], [UCG loadable modules])

AC_CONFIG_FILES([src/ucg/Makefile
                 src/ucg/api/ucg_version.h
                 src/ucg/base/ucg_version.c])
