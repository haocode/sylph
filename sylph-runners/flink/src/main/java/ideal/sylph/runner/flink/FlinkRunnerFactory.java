package ideal.sylph.runner.flink;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import ideal.sylph.common.bootstrap.Bootstrap;
import ideal.sylph.runner.flink.actuator.FlinkStreamEtlActuator;
import ideal.sylph.runner.flink.actuator.FlinkStreamSqlActuator;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.RunnerFactory;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.model.PipelinePluginManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.tree.ClassTypeSignature;

import java.io.File;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;
//获取注入 FlinkRunner FlinkStreamEtlActuator  FlinkStreamSqlActuator  FlinkYarnJobLauncher
public class FlinkRunnerFactory
        implements RunnerFactory
{
    private static final Logger logger = LoggerFactory.getLogger(FlinkRunnerFactory.class);

    @Override
    public Runner create(RunnerContext context)
    {
        requireNonNull(context, "context is null");
        String flinkHome = requireNonNull(System.getenv("FLINK_HOME"), "FLINK_HOME not setting");
        checkArgument(new File(flinkHome).exists(), "FLINK_HOME " + flinkHome + " not exists");

        final ClassLoader classLoader = this.getClass().getClassLoader();
        try {
            if (classLoader instanceof DirClassLoader) {
                ((DirClassLoader) classLoader).addDir(new File(flinkHome, "lib"));
            }


            //new Thread(new Runnable() {@Override
            //    public void run() {
            //        System.out.println("Hello World!");}
            //});  使用Lambda表达式则只需要使用一句话就可代替上面使用匿名类的方式  new Thread(() -> System.out.println("Hello World!"));
            //在这个例子中，传统的语法规则，我们是将一个匿名内部类作为参数进行传递，我们实现了Runnable接口，并将其作为参数传递给Thread类，这实际上我们传递的是一段代码，也即我们将代码作为了数据进行传递，这就带来许多不必要的“样板代码”
            //能够接收Lambda表达式的参数类型，是一个只包含一个方法的接口。只包含一个方法的接口称之为“函数接口”
            //关注Lambda表达式“(x) -> Sysout.out.println("Hello World" + x)”，左边传递的是参数，此处并没有指明参数类型，因为它可以通过上下文进行类型推导，但在有些情况下不能推导出参数类型（在编译时不能推导通常IDE会提示），此时则需要指明参数类型
            //count = studentList.stream().filter((student -> student.getCity().equals("chengdu"))).count();
            //
            Bootstrap app = new Bootstrap(new FlinkRunnerModule(), binder -> {
                //只要在类上加上这个注解，就可以实现一个单例类，不需要自己手动编写单例实现类。@Named注解提供了为属性赋值的功能
                binder.bind(FlinkRunner.class).in(Scopes.SINGLETON);
                binder.bind(FlinkStreamEtlActuator.class).in(Scopes.SINGLETON);
                binder.bind(FlinkStreamSqlActuator.class).in(Scopes.SINGLETON);
                binder.bind(FlinkYarnJobLauncher.class).in(Scopes.SINGLETON);
                //----------------------------------
                binder.bind(PipelinePluginManager.class)
                        //() -> createPipelinePluginManager(context) ()左边代表参数 -> 右边代表函数主体
                        .toProvider(() -> createPipelinePluginManager(context))
                        .in(Scopes.SINGLETON);
            });
            Injector injector = app.strictConfig()
                    .setRequiredConfigurationProperties(Collections.emptyMap())
                    .initialize();
            return injector.getInstance(FlinkRunner.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private static PipelinePluginManager createPipelinePluginManager(RunnerContext context)
    {
        Set<String> keyword = Stream.of(
                org.apache.flink.table.api.StreamTableEnvironment.class,
                org.apache.flink.table.api.java.StreamTableEnvironment.class,
                org.apache.flink.table.api.scala.StreamTableEnvironment.class,
                org.apache.flink.streaming.api.datastream.DataStream.class
        ).map(Class::getName).collect(Collectors.toSet());

        Set<PipelinePluginManager.PipelinePluginInfo> flinkPlugin = context.getFindPlugins().stream().filter(it -> {
            if (it.getRealTime()) {
                return true;
            }
            if (it.getJavaGenerics().length == 0) {
                return false;
            }
            ClassTypeSignature typeSignature = (ClassTypeSignature) it.getJavaGenerics()[0];
            String typeName = typeSignature.getPath().get(0).getName();
            return keyword.contains(typeName);
        }).collect(Collectors.toSet());
        return new PipelinePluginManager()
        {
            @Override
            public Set<PipelinePluginInfo> getAllPlugins()
            {
                return flinkPlugin;
            }
        };
    }
}
