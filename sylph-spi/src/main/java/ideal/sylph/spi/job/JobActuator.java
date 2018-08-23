package ideal.sylph.spi.job;

import java.net.URLClassLoader;
//作业执行机构
public interface JobActuator
{
    JobActuatorHandle getHandle();

    ActuatorInfo getInfo();

    URLClassLoader getHandleClassLoader();

    interface ActuatorInfo
    {
        String[] getName();

        String getDescription();

        long getCreateTime();

        String getVersion();
    }
}
