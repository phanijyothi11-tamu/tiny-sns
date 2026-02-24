#
# CSCE 438 - MP2 Build File
# Supports: coordinator, tsd (SNS server), tsc (SNS client)
#

# ---- GRPC / PROTOBUF DEPENDENCIES ----
PROTOBUF_ABSL_DEPS = absl_absl_check absl_absl_log absl_algorithm absl_base absl_bind_front absl_bits absl_btree absl_cleanup absl_cord absl_core_headers absl_debugging absl_die_if_null absl_dynamic_annotations absl_flags absl_flat_hash_map absl_flat_hash_set absl_function_ref absl_hash absl_layout absl_log_initialize absl_log_severity absl_memory absl_node_hash_map absl_node_hash_set absl_optional absl_span absl_status absl_statusor absl_strings absl_synchronization absl_time absl_type_traits absl_utility absl_variant
PROTOBUF_UTF8_RANGE_LINK_LIBS = -lutf8_validity

export PKG_CONFIG_PATH=/home/csce438/.local/lib/pkgconfig:/home/csce438/grpc/third_party/re2:/home/csce438/.local/share/pkgconfig

HOST_SYSTEM = $(shell uname | cut -f1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CXX = g++
CPPFLAGS += $(shell pkg-config --cflags protobuf grpc absl_flags absl_flags_parse)
CXXFLAGS += -std=c++17

ifeq ($(SYSTEM),Darwin)
LDFLAGS += -L/usr/local/lib $(shell pkg-config --libs --static protobuf grpc++ absl_flags absl_flags_parse $(PROTOBUF_ABSL_DEPS)) \
           $(PROTOBUF_UTF8_RANGE_LINK_LIBS) \
           -pthread -lgrpc++_reflection -ldl
else
LDFLAGS += -L/usr/local/lib $(shell pkg-config --libs --static protobuf grpc++ absl_flags absl_flags_parse $(PROTOBUF_ABSL_DEPS)) \
           $(PROTOBUF_UTF8_RANGE_LINK_LIBS) \
           -pthread -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed -ldl -lglog
endif

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= $(shell which $(GRPC_CPP_PLUGIN))

PROTOS_PATH = .
vpath %.proto $(PROTOS_PATH)

# ---- TARGETS ----
.PHONY: all clean system-check

all: system-check coordinator tsd tsc

# ----- Coordinator -----
coordinator: coordinator.o coordinator.pb.o coordinator.grpc.pb.o
	$(CXX) $^ $(LDFLAGS) -g -o $@

# ----- SNS Server (TSD) -----
tsd: coordinator.pb.o coordinator.grpc.pb.o sns.pb.o sns.grpc.pb.o tsd.o
	$(CXX) $^ $(LDFLAGS) -g -o $@

# ----- SNS Client (TSC) -----
tsc: coordinator.pb.o coordinator.grpc.pb.o client.o sns.pb.o sns.grpc.pb.o tsc.o
	$(CXX) $^ $(LDFLAGS) -g -o $@

# ---- PROTOBUF / GRPC FILE GENERATION RULES ----
.PRECIOUS: %.grpc.pb.cc %.pb.cc

%.pb.cc %.pb.h: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

%.grpc.pb.cc %.grpc.pb.h: %.proto
	$(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<

# ---- COMPILE RULES ----
%.o: %.cc %.pb.h %.grpc.pb.h
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -c $< -o $@

# ---- CLEAN ----
clean:
	rm -f *~ *.o *.pb.cc *.pb.h *.grpc.pb.cc *.grpc.pb.h tsc tsd coordinator

# ---- DEPENDENCY CHECK ----
PROTOC_CMD = which $(PROTOC)
PROTOC_CHECK_CMD = $(PROTOC) --version | grep -q 'libprotoc.3\|libprotoc [0-9][0-9]\.'
PLUGIN_CHECK_CMD = which $(GRPC_CPP_PLUGIN)
HAS_PROTOC = $(shell $(PROTOC_CMD) > /dev/null && echo true || echo false)
ifeq ($(HAS_PROTOC),true)
HAS_VALID_PROTOC = $(shell $(PROTOC_CHECK_CMD) 2> /dev/null && echo true || echo false)
endif
HAS_PLUGIN = $(shell $(PLUGIN_CHECK_CMD) > /dev/null && echo true || echo false)

SYSTEM_OK = false
ifeq ($(HAS_VALID_PROTOC),true)
ifeq ($(HAS_PLUGIN),true)
SYSTEM_OK = true
endif
endif

system-check:
ifneq ($(HAS_VALID_PROTOC),true)
	@echo " DEPENDENCY ERROR"
	@echo
	@echo "You don't have protoc 3.0.0 or newer installed in your path."
	@echo "Please install an up-to-date version of Google protocol buffers."
	@echo
	@echo "   https://github.com/protocolbuffers/protobuf/releases"
	@echo
	-$(PROTOC) --version
endif
ifneq ($(HAS_PLUGIN),true)
	@echo " DEPENDENCY ERROR"
	@echo
	@echo "You don't have the grpc c++ protobuf plugin installed in your path."
	@echo
	@echo "   https://github.com/grpc/grpc"
	@echo
	-which $(GRPC_CPP_PLUGIN)
endif
ifneq ($(SYSTEM_OK),true)
	@false
endif
