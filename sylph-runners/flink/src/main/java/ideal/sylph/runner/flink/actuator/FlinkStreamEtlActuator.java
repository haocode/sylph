package ideal.sylph.runner.flink.actuator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import ideal.sylph.annotation.Description;
import ideal.sylph.annotation.Name;
import ideal.sylph.common.proxy.DynamicProxy;
import ideal.sylph.runner.flink.yarn.FlinkYarnJobLauncher;
import ideal.sylph.spi.EtlFlow;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.classloader.ThreadContextClassLoader;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuatorHandle;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobHandle;
import ideal.sylph.spi.model.NodeInfo;
import ideal.sylph.spi.model.PipelinePluginManager;
import ideal.sylph.spi.utils.GenericTypeReference;
import ideal.sylph.spi.utils.JsonTextUtil;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static ideal.sylph.spi.exception.StandardErrorCode.JOB_BUILD_ERROR;
import static java.util.Objects.requireNonNull;



@Name("StreamETL")
@Description("this is stream etl Actuator")
public class FlinkStreamEtlActuator
        implements JobActuatorHandle
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(FlinkStreamEtlActuator.class);
    @Inject private FlinkYarnJobLauncher jobLauncher;
    @Inject private PipelinePluginManager pluginManager;

    @NotNull
    @Override
    public JobHandle formJob(String jobId, Flow inFlow, DirClassLoader jobClassLoader)
            throws IOException
    {
        EtlFlow flow = (EtlFlow) inFlow;
        //---- flow parser depends ----
        ImmutableSet.Builder<File> builder = ImmutableSet.builder();
        for (NodeInfo nodeInfo : flow.getNodes()) {
            String json = JsonTextUtil.readJsonText(nodeInfo.getNodeText());
            Map<String, Object> nodeConfig = nodeInfo.getNodeConfig();
            Map<String, Object> config = MAPPER.readValue(json, new GenericTypeReference(Map.class, String.class, Object.class));
            String driverString = (String) requireNonNull(config.get("driver"), "driver is null");
            Optional<PipelinePluginManager.PipelinePluginInfo> pluginInfo = pluginManager.findPluginInfo(driverString);
            //Optional<PipelinePluginManager.PipelinePluginInfo> pluginInfo = runner.getPluginManager().findPluginInfo(driverString);
            pluginInfo.ifPresent(plugin -> FileUtils.listFiles(plugin.getPluginFile(), null, true).forEach(builder::add));
        }
        jobClassLoader.addJarFiles(builder.build());
        try {
            return FlinkJobUtil.createJob(jobId, flow, jobClassLoader, pluginManager);
        }
        catch (Exception e) {
            throw new SylphException(JOB_BUILD_ERROR, e);
        }
    }
//这里获取yarnAppId 打印相关信息 提交任务
    @Override
    public JobContainer createJobContainer(@NotNull Job job, String jobInfo)
    {
        JobContainer yarnJobContainer = new YarnJobContainer(jobLauncher.getYarnClient(), jobInfo)
        {
            @Override
            public Optional<String> run()
                    throws Exception
            {
                ApplicationId yarnAppId = jobLauncher.createApplication();
                this.setYarnAppId(yarnAppId);
                logger.info("Instantiating flinkSqlJob {} at yarnId {}", job.getId(), yarnAppId);
                jobLauncher.start(job, yarnAppId);
                return Optional.of(yarnAppId.toString());
            }
        };
        //----create JobContainer Proxy
        DynamicProxy invocationHandler = new DynamicProxy(yarnJobContainer)
        {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args)
                    throws Throwable
            {
                /*
                 * 通过这个 修改当前YarnClient的ClassLoader的为当前sdk的加载器
                 * 默认hadoop Configuration使用jvm的AppLoader,会出现 akka.version not setting的错误 原因是找不到akka相关jar包
                 * 原因是hadoop Configuration 初始化: this.classLoader = Thread.currentThread().getContextClassLoader();
                 * */
                try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(this.getClass().getClassLoader())) {
                    return method.invoke(yarnJobContainer, args);
                }
            }
        };

        return (JobContainer) invocationHandler.getProxy(JobContainer.class);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", "streamSql")
                .add("description", ".....")
                .toString();
    }
}
