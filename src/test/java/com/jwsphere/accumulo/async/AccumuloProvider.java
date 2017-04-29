package com.jwsphere.accumulo.async;

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

}
