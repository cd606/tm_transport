project('TM kit transport', 'cpp', default_options: ['cpp_std=c++17'])

inc = include_directories('.')
thread_dep = dependency('threads')
boost_dep = dependency('boost')
rabbitmq_dep = dependency('librabbitmq')
zmq_dep = [dependency('libzmq'), dependency('cppzmq')]
redis_dep = [dependency('hiredis')]
nng_dep = [dependency('nngpp')]
crossguid_dep = dependency('crossguid')
grpc_dep = dependency('grpc')
etcd_dep = dependency('libetcdcpp')
soci_dep = dependency('soci')
libsodium_dep = dependency('libsodium')
tm_infra_dep = dependency('tm_kit_infra')
tm_basic_dep = dependency('tm_kit_basic')

common_deps = [thread_dep, boost_dep, rabbitmq_dep, zmq_dep, redis_dep, nng_dep, crossguid_dep, grpc_dep, etcd_dep, soci_dep, libsodium_dep, tm_infra_dep, tm_basic_dep]

if build_machine.system() == 'windows'
    common_deps += declare_dependency(link_args: 'bcrypt.lib')
endif

if get_option('grpc_interop').disabled()
    add_global_arguments('-DTM_KIT_TRANSPORT_GRPC_INTEROP_DISABLED', language: 'cpp')
endif

pkg = import('pkgconfig')

subdir('tm_kit/transport')
subdir('src')
