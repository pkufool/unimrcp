AM_CPPFLAGS                = $(UNIMRCP_PLUGIN_INCLUDES)

plugin_LTLIBRARIES         = demorecog.la

demorecog_la_SOURCES       = src/demo_recog_engine.c
demorecog_la_LDFLAGS       = $(UNIMRCP_PLUGIN_OPTS) -lwebsockets -ljson-c -lpthread

include $(top_srcdir)/build/rules/uniplugin.am
