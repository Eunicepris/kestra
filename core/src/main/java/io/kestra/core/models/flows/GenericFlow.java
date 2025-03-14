package io.kestra.core.models.flows;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.kestra.core.exceptions.DeserializationException;
import io.kestra.core.models.HasSource;
import io.kestra.core.models.HasUID;
import io.kestra.core.models.Label;
import io.kestra.core.serializers.ListOrMapOfLabelDeserializer;
import io.kestra.core.serializers.ListOrMapOfLabelSerializer;
import io.kestra.core.serializers.YamlParser;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents an un-typed {@link FlowInterface} implementation for which
 * most properties are backed by a {@link Map}.
 *
 * <p>
 * This implementation should be preferred over other implementations when
 * no direct access to tasks and triggers is required.
 */
@SuperBuilder(toBuilder = true)
@Getter
@NoArgsConstructor
public class GenericFlow implements FlowInterface, HasSource, HasUID {

    String id;

    String namespace;

    Integer revision;

    @Builder.Default
    boolean disabled = false;

    @Builder.Default
    boolean deleted = false;

    @JsonSerialize(using = ListOrMapOfLabelSerializer.class)
    @JsonDeserialize(using = ListOrMapOfLabelDeserializer.class)
    @Schema(implementation = Object.class, oneOf = {List.class, Map.class})
    List<Label> labels;

    String tenantId;

    @JsonIgnore
    private String source;

    @JsonIgnore
    @Builder.Default
    private Map<String, Object> additionalProperties = new HashMap<>();

    /**
     * Static helper method for constructing a {@link GenericFlow} from {@link Flow}.
     *
     * @param flow The flow.
     * @return a new {@link GenericFlow}
     * @throws DeserializationException if source cannot be deserialized.
     */
    public static GenericFlow of(final Flow flow) throws DeserializationException {
        if (flow instanceof FlowWithSource f) {
            return fromYaml(flow.getTenantId(), f.source);
        } else {
            return fromYaml(flow.getTenantId(), flow.generateSource());
        }
    }

    /**
     * Static helper method for constructing a {@link GenericFlow} from a YAML source.
     *
     * @param source The flow YAML source.
     * @return a new {@link GenericFlow}
     * @throws DeserializationException if source cannot be deserialized.
     */
    public static GenericFlow fromYaml(final String source) throws DeserializationException {
        return fromYaml(null, source);
    }

    /**
     * Static helper method for constructing a {@link GenericFlow} from a YAML source.
     *
     * @param source The flow YAML source.
     * @return a new {@link GenericFlow}
     * @throws DeserializationException if source cannot be deserialized.
     */
    public static GenericFlow fromYaml(final String tenantId, final String source) throws DeserializationException {
        GenericFlow parsed = YamlParser.parse(source, GenericFlow.class);
        return parsed.toBuilder()
            .tenantId(tenantId)
            .source(source)
            .build();
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String source() {
        return source;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String uid() {
        return Flow.uid(this);
    }

    public GenericFlow tenantId(final String tenantId) {
        // This is a hack to set the tenantId in template tasks.
        // When using the Template task, we need the tenantId to fetch the Template from the database.
        // However, as the task is executed on the Executor we cannot retrieve it from the tenant service and have no other options.
        // So we save it at flow creation/updating time.
        // TODO

        return this.toBuilder().tenantId(tenantId).build();
    }
}
