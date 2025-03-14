package io.kestra.core.models.flows;

import io.kestra.core.models.DeletedInterface;
import io.kestra.core.models.TenantInterface;

import java.util.List;

/**
 * The base interface for FLow.
 */
public interface FlowInterface extends DeletedInterface, TenantInterface {
    String getId();

    String getNamespace();

    Integer getRevision();

    boolean isDisabled();

    String getTenantId();

    boolean isDeleted();
}
