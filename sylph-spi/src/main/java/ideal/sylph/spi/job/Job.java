package ideal.sylph.spi.job;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.net.URL;
import java.util.Collection;

public interface Job
{
    //javax.validation 是一套JavaBean参数校验的标准，它定义了很多常用的校验注解，
    // 我们可以直接将这些注解加在我们JavaBean的属性上面，就可以在需要校验的时候进行校验了
    @NotNull
    public String getId();

    default String getDescription()
    {
        return "none";
    }

    File getWorkDir();

    Collection<URL> getDepends();


    //获取任务执行机构名称
    @NotNull
    String getActuatorName();

    @NotNull
    JobHandle getJobHandle();

    @NotNull
    Flow getFlow();

    public enum Status
    {
        RUNNING(0),   //运行中
        STARTING(1),    // 启动中
        STOP(2),           // 停止运行
        START_ERROR(3);           // 启动失败

        private final int status;

        Status(int code)
        {
            this.status = code;
        }
    }
}
