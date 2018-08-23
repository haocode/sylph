package ideal.sylph.main.service;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import ideal.sylph.etl.PipelinePlugin;
import ideal.sylph.spi.Runner;
import ideal.sylph.spi.RunnerContext;
import ideal.sylph.spi.RunnerFactory;
import ideal.sylph.spi.classloader.DirClassLoader;
import ideal.sylph.spi.classloader.ThreadContextClassLoader;
import ideal.sylph.spi.job.Flow;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;
import ideal.sylph.spi.job.JobHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * RunnerManager
 *
 */
public class RunnerManager
{
    private static final Logger logger = LoggerFactory.getLogger(RunnerManager.class);
    private final Map<String, JobActuator> jobActuatorMap = new HashMap<>();
    private final Map<String, Runner> runnerMap = new HashMap<>();
    private final PipelinePluginLoader pluginLoader;

    @Inject
    public RunnerManager(PipelinePluginLoader pluginLoader)
    {
        this.pluginLoader = requireNonNull(pluginLoader, "pluginLoader is null");
    }

    public void createRunner(RunnerFactory factory)
    {                                   //::调用pluginLoader对象方法
        RunnerContext runnerContext = pluginLoader::getPluginsInfo;
        logger.info("===== Runner: {} starts loading {} =====", factory.getClass().getName(), PipelinePlugin.class.getName());

        final Runner runner = factory.create(runnerContext);
        runner.getJobActuators().forEach(jobActuatorHandle -> {
            JobActuator jobActuator = new JobActuatorImpl(jobActuatorHandle);
            for (String name : jobActuator.getInfo().getName()) {
                if (jobActuatorMap.containsKey(name)) {
                    throw new IllegalArgumentException(String.format("Multiple entries with same key: %s=%s and %s=%s", name, jobActuatorMap.get(name), name, jobActuator));
                }
                else {
                    jobActuatorMap.put(name, jobActuator);
                    runnerMap.put(name, runner);
                }
            }
        });
    }

    /**
     * 创建job 运行时
     */
    JobContainer createJobContainer(@Nonnull Job job, String jobInfo)
    {
        String jobType = requireNonNull(job.getActuatorName(), "job Actuator Name is null " + job.getId());
        JobActuator jobActuator = jobActuatorMap.get(jobType);
        checkArgument(jobActuator != null, jobType + " not exists");
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(jobActuator.getHandleClassLoader())) {
            return jobActuator.getHandle().createJobContainer(job, jobInfo);
        }
    }

    public Job formJobWithFlow(String jobId, byte[] flowBytes, String actuatorName)
            throws IOException
    {
        requireNonNull(actuatorName, "job actuatorName is null");
        Runner runner = runnerMap.get(actuatorName);
        JobActuator jobActuator = jobActuatorMap.get(actuatorName);
        checkArgument(jobActuator != null, "job [" + jobId + "] loading error! JobActuator:[" + actuatorName + "] not find,only " + jobActuatorMap.keySet());
        checkArgument(runner != null, "Unable to find runner for " + actuatorName);
        File jobDir = new File("jobs/" + jobId);
        try (DirClassLoader jobClassLoader = new DirClassLoader(null, jobActuator.getHandleClassLoader())) {
            jobClassLoader.addDir(jobDir);
            Flow flow = jobActuator.getHandle().formFlow(flowBytes);
            JobHandle jobHandle = jobActuator.getHandle().formJob(jobId, flow, jobClassLoader);
            Collection<URL> dependFiles = getJobDependFiles(jobClassLoader);
            return new Job()
            {
                @NotNull
                @Override
                public String getId()
                {
                    return jobId;
                }

                @Override
                public File getWorkDir()
                {
                    return jobDir;
                }

                @Override
                public Collection<URL> getDepends()
                {
                    return dependFiles;
                }

                @NotNull
                @Override
                public String getActuatorName()
                {
                    return actuatorName;
                }

                @NotNull
                @Override
                public JobHandle getJobHandle()
                {
                    return jobHandle;
                }

                @NotNull
                @Override
                public Flow getFlow()
                {
                    return flow;
                }
            };
        }
    }

    public Collection<JobActuator.ActuatorInfo> getAllActuatorsInfo()
    {
        return jobActuatorMap.values()
                .stream()
                .distinct().map(JobActuator::getInfo)
                .collect(Collectors.toList());
    }

    private static Collection<URL> getJobDependFiles(final ClassLoader jobClassLoader)
    {
        ImmutableList.Builder<URL> builder = ImmutableList.builder();
        if (jobClassLoader instanceof URLClassLoader) {
            builder.add(((URLClassLoader) jobClassLoader).getURLs());

            final ClassLoader parentClassLoader = jobClassLoader.getParent();
            if (parentClassLoader instanceof URLClassLoader) {
                builder.add(((URLClassLoader) parentClassLoader).getURLs());
            }
        }
        return builder.build().stream().collect(Collectors.toMap(URL::getPath, v -> v, (x, y) -> y))  //distinct
                .values().stream().sorted(Comparator.comparing(URL::getPath))
                .collect(Collectors.toList());
    }
}
