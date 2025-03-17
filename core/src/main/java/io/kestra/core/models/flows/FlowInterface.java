package io.kestra.core.models.flows;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.kestra.core.models.DeletedInterface;
import io.kestra.core.models.HasUID;
import io.kestra.core.models.Label;
import io.kestra.core.models.TenantInterface;

import java.util.List;
import java.util.Map;

/**
 * The base interface for FLow.
 */
@JsonDeserialize(as = GenericFlow.class)
public interface FlowInterface extends DeletedInterface, TenantInterface, HasUID {
    String getId();

    String getNamespace();

    Integer getRevision();

    boolean isDisabled();

    String getTenantId();

    boolean isDeleted();

    List<Label> getLabels();

    List<Input<?>> getInputs();

    List<Output> getOutputs();

    Map<String, Object> getVariables();

    default String source() {
        return null;
    }

    @Override
    @JsonIgnore
    default String uid() {
        return Flow.uid(this);
    }

    @JsonIgnore
    default String uidWithoutRevision() {
        return Flow.uidWithoutRevision(this);
    }
}
