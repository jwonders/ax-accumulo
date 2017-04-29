package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class MiniAccumuloProvider implements AccumuloProvider {

    @Override
    public String getAdminUser() {
        return MiniAccumuloProviderInstance.INSTANCE.getAdminUser();
    }

    @Override
    public AuthenticationToken getAdminToken() {
        return MiniAccumuloProviderInstance.INSTANCE.getAdminToken();
    }

    @Override
    public MiniAccumuloCluster getAccumuloCluster() {
        return MiniAccumuloProviderInstance.INSTANCE.getAccumuloCluster();
    }

    private enum MiniAccumuloProviderInstance implements AccumuloProvider {

        INSTANCE;

        private MiniAccumuloCluster cluster;

        MiniAccumuloProviderInstance() {
            try {
                File file = Files.createTempDirectory("minicluster").toFile();
                MiniAccumuloConfig config = new MiniAccumuloConfig(file, "root");
                cluster = new MiniAccumuloCluster(config);
                cluster.start();
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        cluster.stop();
                    } catch (IOException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } catch (Throwable e) {
                e.printStackTrace();
                throw e;
            }
        }


        @Override
        public String getAdminUser() {
            return "root";
        }

        @Override
        public AuthenticationToken getAdminToken() {
            return new PasswordToken("root");
        }

        @Override
        public MiniAccumuloCluster getAccumuloCluster() {
            return cluster;
        }

    }

}
