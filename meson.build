project('TM kit transport', 'cpp', default_options: ['cpp_std=c++17', 'warning_level=3'])

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
boost_certify_dep = dependency('boost_certify')
curlpp_dep = dependency('curlpp')
ixwebsocket_dep = dependency('ixwebsocket')
if get_option('buildtype') == 'debug'
    tm_infra_dep = dependency('tm_kit_infra_debug')
    tm_basic_dep = dependency('tm_kit_basic_debug')
else
    tm_infra_dep = dependency('tm_kit_infra')
    tm_basic_dep = dependency('tm_kit_basic')
endif

common_deps = [thread_dep, boost_dep, rabbitmq_dep, zmq_dep, redis_dep, nng_dep, crossguid_dep, grpc_dep, etcd_dep, soci_dep, libsodium_dep, boost_certify_dep, tm_infra_dep, tm_basic_dep, curlpp_dep, ixwebsocket_dep]

if build_machine.system() == 'windows'
    common_deps += declare_dependency(link_args: 'bcrypt.lib')
    common_deps += declare_dependency(link_args: 'crypt32.lib')
    add_global_arguments('/bigobj', language: 'cpp')
    add_global_arguments('-DWINVER=0x0601', language: 'cpp')
    add_global_arguments('-D_WIN32_WINNT=0x0601', language: 'cpp')
    add_global_arguments('-DBOOST_USE_WINAPI_VERSION=BOOST_WINAPI_VERSION_WIN7', language: 'cpp')
endif

pkg = import('pkgconfig')

subdir('build_environment_tools')
subdir('tm_kit/transport')
subdir('src')
