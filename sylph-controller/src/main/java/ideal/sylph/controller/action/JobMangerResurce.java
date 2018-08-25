package ideal.sylph.controller.action;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import ideal.sylph.common.base.Throwables;
import ideal.sylph.spi.SylphContext;
import ideal.sylph.spi.job.JobContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static ideal.sylph.spi.job.Job.Status.STOP;
import static java.util.Objects.requireNonNull;

@javax.inject.Singleton
//@javax.ws.rs.PathParam： 从URI模板参数中提取数据
@Path("/job_manger")
public class JobMangerResurce
{
    private static final Logger logger = LoggerFactory.getLogger(JobMangerResurce.class);

    @Context private ServletContext servletContext;

    //@javax.ws.rs.core.Context 注释指示注入了上下文对象。javax.ws.rs.core.UriInfo 接口是您要注入的对象的接口。 您可以利用 UriBuilder 类，使用 UriInfo 对象来构建绝对和相对 URL。
    @Context private UriInfo uriInfo;
    private SylphContext sylphContext;

    public JobMangerResurce(
            //@javax.ws.rs.core.Context 通用的注入annotation，允许注入各种帮助或者信息对象
            //有时候可能想通过程序的方式获取URI中的信息，而不使用PathParam注释。这里我们需要通过接口javax.ws.rs.core.UriInfo接口去获取这些信息
            // 要获取UriInfo对象，就需要用到@javax.ws.rs.core.Context注释了
            @Context ServletContext servletContext,
            @Context UriInfo uriInfo)
    {
        this.servletContext = servletContext;
        this.uriInfo = uriInfo;
        this.sylphContext = (SylphContext) servletContext.getAttribute("sylphContext");
    }

    @Path("/get_all_actuators")
    @GET
    //Produces 标注返回的MIME媒体类型
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    // @QueryParam("id")    获取参数上的参数
    public List<String> getAllActuators(@QueryParam("name") String name)
    {
        //test Object a1 = uriInfo.getQueryParameters();
        List<String> names = sylphContext.getAllActuatorsInfo().stream().flatMap(x -> Arrays.stream(x.getName())).collect(Collectors.toList());
        return names;
    }

    @POST
    //Consumes 标注可接受请求的MIME媒体类型
    @Consumes({MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN})
    @Produces({MediaType.APPLICATION_JSON})
    public Map doPostHandler(Body body)
    {
        switch (body.getType()) {
            case "refresh_all":  //刷新
            case "list":  //获取列表
                return listJobs();
            case "stop":  //下线应用
                sylphContext.stopJob(body.getJobId());
                break;
            case "active": //启动任务
                sylphContext.startJob(body.getJobId());
                break;
            case "delete": //删除任务
                sylphContext.deleteJob(body.getJobId());
                break;
            default:
                break;
        }

        return ImmutableMap.of();
    }

    private Map listJobs()
    {
        final List<Object> outData = new ArrayList<>();
        try {
            sylphContext.getAllJobs().forEach(job -> {
                String jobId = job.getId();
                Optional<JobContainer> jobContainer = sylphContext.getJobContainer(jobId);

                Map<String, Object> line = new HashMap<>();
                line.put("status", STOP);  //默认为未上线
                line.put("jobId", jobId);
                line.put("type", job.getActuatorName());
                line.put("create_time", 0);  //getUserModuleManger().getCount("action")
                //isPresent() 与 obj != null 无任何分别
                //Optional<JobContainer>
                jobContainer.ifPresent(container -> {
                    line.put("yarnId", container.getRunId());
                    line.put("status", container.getStatus());
                    line.put("app_url", "/proxy/" + jobId + "/#");
                });
                outData.add(line);
            });
            return ImmutableMap.of("data", outData);
        }
        catch (Exception e) {
            logger.error("", Throwables.getRootCause(e));
            throw new RuntimeException(Throwables.getRootCause(e));
        }
    }
    //接口的数据我无法完全的预知，所以实体类字段有可能不完整。所以当返回的json数据里包含了实体类没有的字段时gson就有可能出错
    //@JsonIgnoreProperties(ignoreUnknown = true)，将这个注解写在类上之后，就会忽略类中不存在的字段，可以满足当前的需要。这个注解还可以指定要忽略的字段
    //Body 请求的实体类
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static final class Body
    {
        private final String type;
        private final String jobId;

        @JsonCreator
        public Body(
                @JsonProperty("type") String type,
                @JsonProperty("jobId") String jobId)
        {
            this.type = requireNonNull(type, "type must not null");
            this.jobId = jobId;
        }

        @JsonProperty
        public String getJobId()
        {
            return requireNonNull(jobId, "jobId must not null");
        }

        @JsonProperty
        public String getType()
        {
            return type;
        }
    }
}
