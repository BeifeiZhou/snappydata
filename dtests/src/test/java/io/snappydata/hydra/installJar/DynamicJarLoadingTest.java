/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.hydra.installJar;

import hydra.FileUtil;
import hydra.Log;
import io.snappydata.hydra.cluster.SnappyBB;
import io.snappydata.hydra.cluster.SnappyPrms;
import io.snappydata.hydra.cluster.SnappyTest;
import org.apache.spark.SnappyTestUtils;
import org.apache.spark.sql.SnappyContext;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import util.TestException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;


public class DynamicJarLoadingTest extends SnappyTest {

    protected static File getTempDir() {
        File log = new File(".");
        String dest = null;
        try {
            dest = log.getCanonicalPath() + File.separator + "temp";
        } catch (IOException e) {
            throw new TestException("IOException occurred while retriving destination temp dir " + log + "\nError Message:" + e.getMessage());
        }
        File tempDir = new File(dest);
        if (!tempDir.exists()) FileUtil.mkdir(tempDir);
        Log.getLogWriter().info("SS - tempDir is : " + tempDir.getAbsolutePath());
        return tempDir;
    }

    public static void HydraTask_installJar() {
        Log.getLogWriter().info("SS - entered into HydraTask_installjar....");
        File file1 = createSupportiveClasses("FakeClass1", "1", getTempDir());
        File file2 = createSupportiveClasses("FakeClass2", "1", getTempDir());
        File file3 = createSupportiveClasses("FakeClass3", "1", getTempDir());
        File file4 = createSupportiveClasses("FakeClass4", "1", getTempDir());
        File file5 = createJobClass("DynamicJarLoadingJob", getTempDir());
        List files = new ArrayList();
        files.add(file1);
        files.add(file2);
        files.add(file3);
        files.add(file4);
        files.add(file5);
        JavaConversions.asScalaBuffer(files);
        SnappyTestUtils.createJarFile((Seq<File>) files, getTempDir());
        Log.getLogWriter().info("SS - Done with HydraTask_installjar....");
    }

    public static void HydraTask_modifyJar() {
        Log.getLogWriter().info("SS - entered into HydraTask_modifyJar....");
        File file1 = createSupportiveClasses("FakeClass1", "2", getTempDir());
        File file2 = createSupportiveClasses("FakeClass2", "2", getTempDir());
        File file3 = createSupportiveClasses("FakeClass5", "2", getTempDir());
        File file4 = createJobClass("DynamicJarLoadingJob", getTempDir());
        List files = new ArrayList();
        JavaConversions.asScalaBuffer(files);
        files.add(file1);
        files.add(file2);
        files.add(file3);
        files.add(file4);
        SnappyTestUtils.createJarFile((Seq<File>) files, getTempDir());
        Log.getLogWriter().info("SS - Done with HydraTask_modifyJar....");
    }

    public static void HydraTask_executeSnappyJobWithDynamicJarLoading() {
        Log.getLogWriter().info("SS - entered into HydraTask_executeSnappyJobWithDynamicJarLoading....");
        int currentThread = snappyTest.getMyTid();
        String logFile = "snappyJobResult_thread_" + currentThread + "_" + System.currentTimeMillis() + ".log";
        SnappyBB.getBB().getSharedMap().put("logFilesForJobs_" + currentThread + "_" + System.currentTimeMillis(), logFile);
        String appJar = getTempDir() + "/testJar-%s.jar";
        Log.getLogWriter().info("SS - appJar location in HydraTask_executeSnappyJobWithDynamicJarLoading is : " + appJar);
        executeSnappyJob(SnappyPrms.getSnappyJobClassNames(), logFile, getTempDir() + "/testJar-%s.jar");
        Log.getLogWriter().info("SS - Done with HydraTask_modifyJar....");
    }

    protected static void executeSnappyJob(Vector jobClassNames, String logFileName, String appJar) {
        String snappyJobScript = snappyTest.getScriptLocation("snappy-job.sh");
        File log = null, logFile = null;
        userAppJar = appJar;
        Log.getLogWriter().info("SS - userAppJar in DynamicJarLoadingTest : " + userAppJar);
        leadHost = snappyTest.getLeadHost();
        try {
            for (int i = 0; i < jobClassNames.size(); i++) {
                String userJob = (String) jobClassNames.elementAt(i);
                String APP_PROPS = null;
                if (SnappyPrms.getCommaSepAPPProps() == null) {
                    APP_PROPS = "logFileName=" + logFileName + ",shufflePartitions=" + SnappyPrms.getShufflePartitions();
                } else {
                    APP_PROPS = SnappyPrms.getCommaSepAPPProps() + ",logFileName=" + logFileName + ",shufflePartitions=" + SnappyPrms.getShufflePartitions();
                }
                String curlCommand1 = "curl --data-binary @" + snappyTest.getUserAppJarLocation(userAppJar) + " " + leadHost + ":" + LEAD_PORT + "/jars/myapp";
                String curlCommand2 = "curl -d " + APP_PROPS + " '" + leadHost + ":" + LEAD_PORT + "/jobs?appName=myapp&classPath=" + userJob + "'";
                ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", curlCommand1);
                log = new File(".");
                String dest = log.getCanonicalPath() + File.separator + logFileName;
                logFile = new File(dest);
                snappyTest.executeProcess(pb, logFile);
                pb = new ProcessBuilder("/bin/bash", "-c", curlCommand2);
                snappyTest.executeProcess(pb, logFile);
            }
            boolean retry = snappyTest.getSnappyJobsStatus(snappyJobScript, logFile);
            if (retry && jobSubmissionCount <= SnappyPrms.getRetryCountForJob()) {
                jobSubmissionCount++;
                Thread.sleep(6000);
                Log.getLogWriter().info("Job failed due to primary lead node failover. Resubmitting the job to new primary lead node.....");
                snappyTest.retrievePrimaryLeadHost();
                HydraTask_executeSnappyJob();
            }
        } catch (IOException e) {
            throw new TestException("IOException occurred while retriving destination logFile path " + log + "\nError Message:" + e.getMessage());
        } catch (InterruptedException e) {
            throw new TestException("Exception occurred while waiting for the snappy streaming job process re-execution." + "\nError Message:" + e.getMessage());
        }
    }

    public static File createSupportiveClasses(String className, String version, File destDir) {
        String generalClasseText = "public class " + className + " implements java.io.Serializable {" +
                "  @Override public String toString() { return \"" + version + "\"; }}";
        Log.getLogWriter().info("SS - destDir in createSupportiveClasses : " + destDir);
        Log.getLogWriter().info("SS - generalClasseText String in createSupportiveClasses is : " + generalClasseText);
        Log.getLogWriter().info("SS - SnappyTestUtils.getJavaSourceFromString in createSupportiveClasses: " + SnappyTestUtils.getJavaSourceFromString(className, generalClasseText));
        return SnappyTestUtils.createCompiledClass(className,
                destDir,
                SnappyTestUtils.getJavaSourceFromString(className, generalClasseText),
                new scala.collection.mutable.ArrayBuffer<URL>());
    }

    public static File createJobClass(String className, File destDir) {
        String generalClasseText = "import com.typesafe.config.Config;import org.apache.spark.sql.SnappyContext;import org.apache.spark.sql.SnappyJobValid;import org.apache.spark.sql.SnappyJobValidation;import org.apache.spark.sql.SnappySQLJob;import java.io.File;import java.io.PrintWriter;import java.io.StringWriter;public class DynamicJarLoadingJob extends SnappySQLJob {  @Override public String toString() { return \"  @Override    public Object runSnappyJob(SnappyContext snc, Config jobConfig) {        try {            PrintWriter pw = new PrintWriter(\"DynamicJarLoadingJob.out\");            pw.println(\"****** DynamicJarLoadingJob started ******\");            String currentDirectory = new File(\".\").getCanonicalPath();            DynamicJarLoadingTest.verify(snc,jobConfig.getString(\"classVersion\"));            pw.println(\"****** DynamicJarLoadingJob finished ******\");            return String.format(\"See %s/JavaCreateAndLoadAirlineDataJob.out\",currentDirectory);        } catch (Exception e) {            StringWriter sw = new StringWriter();            PrintWriter spw = new PrintWriter(sw);            spw.println(\"ERROR: failed with \" + e);            e.printStackTrace(spw);            return spw.toString(); } } @Override    public SnappyJobValidation isValidJob(SnappyContext snc, Config config) {        return new SnappyJobValid();    }\"}}";
        Log.getLogWriter().info("SS - destDir in createJobClass : " + destDir);
        Log.getLogWriter().info("SS - generalClasseText String is : " + generalClasseText);
        Log.getLogWriter().info("SS - SnappyTestUtils.getJavaSourceFromString : " + SnappyTestUtils.getJavaSourceFromString(className, generalClasseText));
        return SnappyTestUtils.createCompiledClass(className,
                destDir,
                SnappyTestUtils.getJavaSourceFromString(className, generalClasseText),
                new scala.collection.mutable.ArrayBuffer<URL>());
    }


    public static void verify(SnappyContext snc, String version) throws Exception {
        int numServers = (int) SnappyBB.getBB().getSharedCounters().read(SnappyBB.numServers);
        Log.getLogWriter().info("SS - SnappyBB.numServers : " + numServers);
        Log.getLogWriter().info("SS - version : " + version);
        /*if (version.equalsIgnoreCase("1")) {
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass1", "1", numServers);
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass2", "1", numServers);
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass3", "1", numServers);
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass4", "1", numServers);
        } else {
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass1", "2", numServers);
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass2", "2", numServers);
            SnappyTestUtils.verifyClassOnExecutors(snc, "FakeClass5", "2", numServers);
        }*/
    }
}
