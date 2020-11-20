package org.apache.dolphinscheduler.common.utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * yarn-site.xml, hdfs-site.xml, core-site.xml配置文件放入conf目录
 */
public class YarnAdapter implements Closeable {

    private static Logger logger = LoggerFactory.getLogger(YarnAdapter.class);

    private static final String YARN_UTILS_KEY = "YARN_UTILS_KEY";

    private YarnClient yarnClient;

    private Configuration conf;

    private static final LoadingCache<String, YarnAdapter> cache = CacheBuilder
            .newBuilder()
            .expireAfterWrite(PropertyUtils.getInt(Constants.KERBEROS_EXPIRE_TIME, 7), TimeUnit.DAYS)
            .build(new CacheLoader<String, YarnAdapter>() {
                @Override
                public YarnAdapter load(String key) throws Exception {
                    return new YarnAdapter();
                }
            });
    public YarnAdapter(){
        conf = new Configuration();
        if (PropertyUtils.getBoolean(Constants.HADOOP_SECURITY_AUTHENTICATION_STARTUP_STATE,false)){
//            conf.addResource(new Path("/home/dolphinscheduler/core-site.xml"));
//        conf.addResource(new Path("/home/dolphinscheduler/hdfs-site.xml"));
//        conf.addResource(new Path("/home/dolphinscheduler/yarn-site.xml"));
            System.setProperty(Constants.JAVA_SECURITY_KRB5_CONF,
                    PropertyUtils.getString(Constants.JAVA_SECURITY_KRB5_CONF_PATH));
            conf.set(Constants.HADOOP_SECURITY_AUTHENTICATION,"kerberos");
            UserGroupInformation.setConfiguration(conf);
//            UserGroupInformation.loginUserFromKeytab(PropertyUtils.getString(Constants.LOGIN_USER_KEY_TAB_USERNAME),
//                    PropertyUtils.getString(Constants.LOGIN_USER_KEY_TAB_PATH));
//
//            UserGroupInformation ugi = UserGroupInformation.createProxyUser("yifanzhang", UserGroupInformation.getLoginUser());
            UserGroupInformation ugi = null;
            try {
                ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                        PropertyUtils.getString(Constants.LOGIN_USER_KEY_TAB_USERNAME), PropertyUtils.getString(Constants.LOGIN_USER_KEY_TAB_PATH));
                yarnClient = ugi.doAs(new PrivilegedExceptionAction<YarnClient>() {
                    @Override
                    public YarnClient run() {
                        yarnClient = YarnClient.createYarnClient();
                        yarnClient.init(conf);
                        yarnClient.start();
                        return yarnClient;
                    }
                });
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        else{
            yarnClient = YarnClient.createYarnClient();
            yarnClient.init(conf);
            yarnClient.start();
        }
    }
    public static YarnAdapter getInstance() {

        return cache.getUnchecked(YARN_UTILS_KEY);
    }
    public YarnApplicationState getApplicationState(ApplicationId applicationId) {


        YarnApplicationState yarnApplicationState = null;
        try {
            ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
            yarnApplicationState = applicationReport.getYarnApplicationState();
        } catch (YarnException e) {
            logger.error("client.getApplications YarnException", e);
        } catch (IOException e) {
            logger.error("client.getApplications IOException", e);
        } finally {
            try {
                yarnClient.close();
            } catch (IOException e) {
                logger.error("client.getApplications IOException", e);
            }
        }

        return yarnApplicationState;
    }

    public void killApplicationId(String appidStr) {
        ApplicationId applicationId = ConverterUtils.toApplicationId(appidStr);
        try {
            yarnClient.killApplication(applicationId);
        } catch (YarnException e) {
            logger.error("client.killApplicationId YarnException", e);
        } catch (IOException e) {
            logger.error("client.killApplicationId IOException", e);
        } finally {
            try {
                yarnClient.close();
            } catch (IOException e) {
                logger.error("client.getApplications IOException", e);
            }
        }

    }


    public ApplicationId getRunningApplicationId(String applicationName) {

        Set<String> applicationTypes = Sets.newHashSet();
        applicationTypes.add("MAPREDUCE");
        applicationTypes.add("SPARK");
        EnumSet<YarnApplicationState> applicationStates = EnumSet.noneOf(YarnApplicationState.class);
        applicationStates.add(YarnApplicationState.ACCEPTED);
        applicationStates.add(YarnApplicationState.SUBMITTED);
        applicationStates.add(YarnApplicationState.RUNNING);
        applicationStates.add(YarnApplicationState.NEW);
        applicationStates.add(YarnApplicationState.NEW_SAVING);

        List<ApplicationReport> applicationReports = null;
        try {
            applicationReports = yarnClient.getApplications(applicationTypes, applicationStates);
        } catch (YarnException e) {
            logger.error("client.getApplications YarnException", e);
        } catch (IOException e) {
            logger.error("client.getApplications IOException", e);
        } finally {
            try {
                yarnClient.close();
            } catch (IOException e) {
                logger.error("client.getApplications IOException", e);
            }
        }

        // 获取最新版本的运行程序

        ApplicationId latestApplicationId = new ApplicationId() {
            @Override
            public int getId() {
                return 0;
            }

            @Override
            protected void setId(int id) {

            }

            @Override
            public long getClusterTimestamp() {
                return 0;
            }

            @Override
            protected void setClusterTimestamp(long clusterTimestamp) {

            }

            @Override
            protected void build() {

            }
        };


        if (CollectionUtils.isNotEmpty(applicationReports)) {
            for (ApplicationReport applicationReport : applicationReports) {
                if (StringUtils.equals(applicationReport.getName(), applicationName)) {

                    if (applicationReport.getApplicationId().compareTo(latestApplicationId) == 1) {
                        latestApplicationId = applicationReport.getApplicationId();
                    }

                }
            }
        }


        return latestApplicationId;

    }

    @Override
    public void close() throws IOException {
        if(this.yarnClient != null){
            this.yarnClient.close();
        }
    }
    public static void main(String[] args) throws Exception {
        YarnAdapter instance = YarnAdapter.getInstance();
        instance.killApplicationId("application_1598310956257_985712");

    }
}
