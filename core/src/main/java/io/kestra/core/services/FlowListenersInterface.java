package io.kestra.core.services;

import io.kestra.core.models.flows.FlowInterface;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface FlowListenersInterface {
    void run();

    void listen(Consumer<List<FlowInterface>> consumer);

    void listen(BiConsumer<FlowInterface, FlowInterface> consumer);

    List<FlowInterface> flows();
}
