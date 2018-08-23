package ideal.sylph.runner.spark.yarn;

import ideal.sylph.spi.exception.SylphException;
import ideal.sylph.spi.job.JobContainerAbs;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;

import java.io.IOException;

import static ideal.sylph.spi.exception.StandardErrorCode.CONNECTION_ERROR;
import static ideal.sylph.spi.job.Job.Status.RUNNING;
import static java.util.Objects.requireNonNull;

public abstract class YarnJobContainer
        extends JobContainerAbs
{
    private ApplicationId yarnAppId;
    private YarnClient yarnClient;

    protected YarnJobContainer(YarnClient yarnClient, String jobInfo)
    {
        this.yarnClient = yarnClient;
        if (jobInfo != null) {
            this.yarnAppId = Apps.toAppID(jobInfo);
            this.setStatus(RUNNING);
        }
    }

    @Override
    public void shutdown()
            throws Exception
    {
        yarnClient.killApplication(yarnAppId);
    }
    //三元运算符 如果满足条件的话就执行第一个结果，如果不满足的话就执行另外一个结果
    @Override
    public String getRunId()
    {
        return yarnAppId == null ? "none" : yarnAppId.toString();
    }

    protected void setYarnAppId(ApplicationId appId)
    {
        this.yarnAppId = requireNonNull(appId, "appId is null");
    }

    @Override
    public boolean isRunning()
    {
        YarnApplicationState yarnAppStatus = getYarnAppStatus(yarnAppId);
        //逻辑或的关系 第一个条件为真时，则不需要执行第二个条件的判断
        return YarnApplicationState.ACCEPTED.equals(yarnAppStatus) || YarnApplicationState.RUNNING.equals(yarnAppStatus);
    }

    @Override
    public String getJobUrl()
    {
        try {
            String originalUrl = yarnClient.getApplicationReport(yarnAppId).getOriginalTrackingUrl();
            return originalUrl;
        }
        catch (YarnException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 获取yarn Job运行情况
     */
    private YarnApplicationState getYarnAppStatus(ApplicationId applicationId)
    {
        try {
            ApplicationReport app = yarnClient.getApplicationReport(applicationId); //获取某个指定的任务
            return app.getYarnApplicationState();
        }
        catch (ApplicationNotFoundException e) {  //app 不存在与yarn上面
            return null;
        }
        catch (YarnException | IOException e) {
            throw new SylphException(CONNECTION_ERROR, e);
        }
    }
}
