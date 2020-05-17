project('TM kit transport', 'cpp', default_options: ['cpp_std=c++17'])

inc = include_directories('.')
thread_dep = dependency('threads')
boost_dep = dependency('boost')
rabbitmq_dep = dependency('librabbitmq')
zmq_dep = [dependency('libzmq'), dependency('cppzmq')]
redis_dep = [dependency('hiredis', required: get_option('redis'))]
simpleAmqpClient_dep = dependency('libSimpleAmqpClient')
tm_infra_dep = dependency('tm_kit_infra')
tm_basic_dep = dependency('tm_kit_basic')

common_deps = [thread_dep, boost_dep, rabbitmq_dep, simpleAmqpClient_dep, zmq_dep, redis_dep, tm_infra_dep, tm_basic_dep]

if build_machine.system() == 'windows'
    common_deps += declare_dependency(link_args: 'bcrypt.lib')
endif

pkg = import('pkgconfig')

subdir('tm_kit/transport')
subdir('src')
