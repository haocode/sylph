package ideal.sylph.runner.flink;

import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import ideal.sylph.runner.flink.yarn.YarnClusterConfiguration;
import ideal.sylph.spi.exception.SylphException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.File;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static ideal.sylph.runner.flink.FlinkRunner.FLINK_DIST;
import static ideal.sylph.spi.exception.StandardErrorCode.CONFIG_ERROR;
import static java.util.Objects.requireNonNull;
//这个FlinkRunnerModule 得到运行相关关联对象
//其实对于Guice而言，程序员所要做的，只是创建一个代表关联关系的Module，然后使用这个Module即可得到对应关联的对象。因此，主要的问题其实就是在如何关联实现类和接口（子类和父类）。
public class FlinkRunnerModule
        implements Module
{
    //在自定义的Module类中进行绑定 在configure方法中绑定 链式绑定 在 Guice 中 Providers 就像 Factories 一样创建和返回对象
    //在使用基于@Provides方法绑定的过程中，如果方法中创建对象的过程很复杂，我们就会考虑，是不是可以把它独立出来，形成一个专门作用的类。Guice提供了一个接口
    //使用toProvider方法来把一种类型绑定到具体的Provider类。当需要相应类型的对象时，Provider类就会调用其get方法获取所需的对象
    @Override
    public void configure(Binder binder)
    {   //:: 相当于set方法 把 loadYarnConfiguration 对象属性赋值给   FlinkRunnerModule 对象
        binder.bind(YarnConfiguration.class).toProvider(FlinkRunnerModule::loadYarnConfiguration).in(Scopes.SINGLETON);
        binder.bind(YarnClient.class).toProvider(YarnClientProvider.class).in(Scopes.SINGLETON);
        binder.bind(YarnClusterConfiguration.class).toProvider(YarnClusterConfigurationProvider.class).in(Scopes.SINGLETON);
    }
    //在 Guice 中 Providers 就像 Factories 一样创建和返回对象。在大部分情况下，客户端可以直接依赖 Guice 框架来为服务（Services）创建依赖的对象。但是少数情况下，应用程序代码需要为一个特定的类型定制对象创建流程（Object creation process），这样可以控制对象创建的数量，提供缓存（Cache）机制等，这样的话我们就要依赖 Guice 的 Provider 类。

    private static class YarnClientProvider
            implements Provider<YarnClient>
    {
        //@Inject，是表示对容器说，这里的YarnConfiguration需要注射，等到运行的时候，容器会拿来一个实例给YarnConfiguration，完成注射的过程
        @Inject private YarnConfiguration yarnConfiguration;

        @Override
        public YarnClient get()
        {
            YarnClient client = YarnClient.createYarnClient();
            client.init(yarnConfiguration);
            client.start();
            return client;
        }
    }

    private static class YarnClusterConfigurationProvider
            implements Provider<YarnClusterConfiguration>
    {
        @Inject private YarnConfiguration yarnConf;

        @Override
        public YarnClusterConfiguration get()
        {
            Path flinkJar = new Path(getFlinkJarFile().toURI());
            @SuppressWarnings("ConstantConditions") final Set<Path> resourcesToLocalize = Stream
                    .of("conf/flink-conf.yaml", "conf/log4j.properties", "conf/logback.xml")
                    .map(x -> new Path(new File(System.getenv("FLINK_HOME"), x).toURI()))
                    .collect(Collectors.toSet());

            String home = "hdfs:///tmp/sylph/apps";
            return new YarnClusterConfiguration(
                    yarnConf,
                    home,
                    flinkJar,
                    resourcesToLocalize);
        }
    }

    private static YarnConfiguration loadYarnConfiguration()
    {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Stream.of("yarn-site.xml", "core-site.xml", "hdfs-site.xml").forEach(file -> {
            File site = new File(System.getenv("HADOOP_CONF_DIR"), file);
            if (site.exists() && site.isFile()) {
                hadoopConf.addResource(new org.apache.hadoop.fs.Path(site.toURI()));
            }
            else {
                throw new SylphException(CONFIG_ERROR, site + "not exists");
            }
        });

        YarnConfiguration yarnConf = new YarnConfiguration(hadoopConf);
        //        try (PrintWriter pw = new PrintWriter(new FileWriter(yarnSite))) { //write local file
        //            yarnConf.writeXml(pw);
        //        }
        return yarnConf;
    }
    //获取额外的依赖jar包
    private static File getFlinkJarFile()
    {
        String flinkHome = requireNonNull(System.getenv("FLINK_HOME"), "FLINK_HOME env not setting");
        if (!new File(flinkHome).exists()) {
            throw new IllegalArgumentException("FLINK_HOME " + flinkHome + " not exists");
        }
        String errorMessage = "error not search " + FLINK_DIST + "*.jar";
        File[] files = requireNonNull(new File(flinkHome, "lib").listFiles(), errorMessage);
        Optional<File> file = Arrays.stream(files)
                .filter(f -> f.getName().startsWith(FLINK_DIST)).findFirst();
        return file.orElseThrow(() -> new IllegalArgumentException(errorMessage));
    }
}
