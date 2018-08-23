package ideal.sylph.main.server;

import ideal.sylph.main.service.JobManager;
import ideal.sylph.main.service.RunnerManager;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.Job;
import ideal.sylph.spi.job.JobActuator;
import ideal.sylph.spi.job.JobContainer;

import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;

import static ideal.sylph.spi.exception.StandardErrorCode.SYSTEM_ERROR;
import static ideal.sylph.spi.exception.StandardErrorCode.UNKNOWN_ERROR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class SylphContextImpl
        implements SylphContext
{
    private JobManager jobManager;
    private RunnerManager runnerManger;

    SylphContextImpl(JobManager jobManager, RunnerManager runnerManger)
    {
        this.jobManager = requireNonNull(jobManager, "jobManager is null");
        this.runnerManger = requireNonNull(runnerManger, "runnerManger is null");
    }

    @Override
    public void saveJob(@NotNull String jobId, @NotNull String flowString, @NotNull String actuatorName)
            throws Exception
    {
        requireNonNull(jobId, "jobId is null");
        requireNonNull(flowString, "flowString is null");
        requireNonNull(actuatorName, "actuatorName is null");
        // 调用 runnerManger 类的方法 返回job相关信息
        Job job = runnerManger.formJobWithFlow(jobId, flowString.getBytes(UTF_8), actuatorName);
        //最终调用 jobStore.saveJob  存储job相关信息
        jobManager.saveJob(job);
    }

    @Override
    public void stopJob(@NotNull String jobId)
    {
        requireNonNull(jobId, "jobId is null");
        try {
            jobManager.stopJob(jobId);
        }
        catch (Exception e) {
            throw new SylphException(UNKNOWN_ERROR, e);
        }
    }

    @Override
    public void startJob(@NotNull String jobId)
    {
        jobManager.startJob(requireNonNull(jobId, "jobId is null"));
    }

    @Override
    public void deleteJob(@NotNull String jobId)
    {
        try {
            jobManager.removeJob(requireNonNull(jobId, "jobId is null"));
        }
        catch (IOException e) {
            throw new SylphException(SYSTEM_ERROR, "drop job " + jobId + " is fail", e);
        }
    }

    @NotNull
    @Override
    public Collection<Job> getAllJobs()
    {
        return jobManager.listJobs();
    }

    @Override
    public Optional<Job> getJob(String jobId)
    {
        return jobManager.getJob(requireNonNull(jobId, "jobId is null"));
    }

    @Override
    public Optional<JobContainer> getJobContainer(@NotNull String jobId)
    {
        return jobManager.getJobContainer(requireNonNull(jobId, "jobId is null"));
    }

    @Override
    public Optional<JobContainer> getJobContainerWithRunId(@NotNull String runId)
    {
        return jobManager.getJobContainerWithRunId(requireNonNull(runId, "runId is null"));
    }

    @Override
    public Collection<JobActuator.ActuatorInfo> getAllActuatorsInfo()
    {
        return runnerManger.getAllActuatorsInfo();
    }
}
