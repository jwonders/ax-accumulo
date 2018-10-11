package com.jwsphere.accumulo.async;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;

public interface AccumuloProvider {

    /**
     * Returns the username of the admin user, typically root.
     */
    String getAdminUser();

    /**
     * Returns the authentication token for the admin user.
     */
    AuthenticationToken getAdminToken();

    /**
     * Returns a reference to the {@code AccumuloCluster}
     */
    MiniAccumuloCluster getAccumuloCluster();

    default Instance newInstance() {
        ClientConfiguration config = ClientConfiguration.create()
                .withInstance(getAccumuloCluster().getInstanceName())
                .withZkHosts(getAccumuloCluster().getZooKeepers());

        return new ZooKeeperInstance(config);
    }

    default Connector newAdminConnector() throws AccumuloException, AccumuloSecurityException {
        return newInstance().getConnector(getAdminUser(), getAdminToken());
    }

}
