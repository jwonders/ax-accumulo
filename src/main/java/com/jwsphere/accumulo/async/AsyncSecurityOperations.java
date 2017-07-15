package com.jwsphere.accumulo.async;

import com.jwsphere.accumulo.async.internal.Unchecked;
import com.jwsphere.accumulo.async.internal.Unchecked.CheckedRunnable;
import com.jwsphere.accumulo.async.internal.Unchecked.CheckedSupplier;
import org.apache.accumulo.core.client.admin.DelegationTokenConfig;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.accumulo.core.client.security.tokens.DelegationToken;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.NamespacePermission;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.core.security.TablePermission;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * An asynchronous interface for performing {@link SecurityOperations}
 *
 * @author Jonathan Wonders
 */
public class AsyncSecurityOperations {

    private final SecurityOperations securityOps;
    private final Executor executor;

    AsyncSecurityOperations(SecurityOperations securityOps, Executor executor) {
        this.securityOps = securityOps;
        this.executor = executor;
    }

    public CompletionStage<Void> createLocalUser(String principal, PasswordToken password) {
        return runAsync(() -> securityOps.createLocalUser(principal, password));
    }

    public CompletionStage<Void> dropLocalUser(String principal) {
        return runAsync(() -> securityOps.dropLocalUser(principal));
    }

    public CompletionStage<Boolean> authenticateUser(String principal, AuthenticationToken token) {
        return supplyAsync(() -> securityOps.authenticateUser(principal, token));
    }

    public CompletionStage<Void> changeLocalUserPassword(String principal, PasswordToken password) {
        return runAsync(() -> securityOps.changeLocalUserPassword(principal, password));
    }

    public CompletionStage<Void> changeUserAuthorizations(String principal, Authorizations authorizations) {
        return runAsync(() -> securityOps.changeUserAuthorizations(principal, authorizations));
    }

    public CompletionStage<Authorizations> getUserAuthorizations(String principal) {
        return supplyAsync(() -> securityOps.getUserAuthorizations(principal));
    }

    public CompletionStage<Boolean> hasSystemPermission(String principal, SystemPermission permission) {
        return supplyAsync(() -> securityOps.hasSystemPermission(principal, permission));
    }

    public CompletionStage<Boolean> hasNamespacePermission(String principal, String namespace, NamespacePermission permission) {
        return supplyAsync(() -> securityOps.hasNamespacePermission(principal, namespace, permission));
    }

    public CompletionStage<Boolean> hasTablePermission(String principal, String table, TablePermission permission) {
        return supplyAsync(() -> securityOps.hasTablePermission(principal, table, permission));
    }

    public CompletionStage<Void> grantSystemPermission(String principal, SystemPermission permission) {
        return runAsync(() -> securityOps.grantSystemPermission(principal, permission));
    }

    public CompletionStage<Void> grantNamespacePermission(String principal, String namespace, NamespacePermission permission) {
        return runAsync(() -> securityOps.grantNamespacePermission(principal, namespace, permission));
    }

    public CompletionStage<Void> grantTablePermission(String principal, String table, TablePermission permission) {
        return runAsync(() -> securityOps.grantTablePermission(principal, table, permission));
    }

    public CompletionStage<Void> revokeSystemPermission(String principal, SystemPermission permission) {
        return runAsync(() -> securityOps.revokeSystemPermission(principal, permission));
    }

    public CompletionStage<Void> revokeNamespacePermission(String principal, String namespace, NamespacePermission permission) {
        return runAsync(() -> securityOps.revokeNamespacePermission(principal, namespace, permission));
    }

    public CompletionStage<Void> revokeTablePermission(String principal, String table, TablePermission permission) {
        return runAsync(() -> securityOps.revokeTablePermission(principal, table, permission));
    }

    public CompletionStage<Set<String>> listLocalUsers() {
        return supplyAsync(securityOps::listLocalUsers);
    }

    public CompletionStage<DelegationToken> getDelegationToken(DelegationTokenConfig cfg) {
        return supplyAsync(() -> securityOps.getDelegationToken(cfg));
    }

    private CompletionStage<Void> runAsync(CheckedRunnable runnable) {
        return Unchecked.runAsync(runnable, executor);
    }

    private <T> CompletionStage<T> supplyAsync(CheckedSupplier<T> supplier) {
        return Unchecked.supplyAsync(supplier, executor);
    }

}
