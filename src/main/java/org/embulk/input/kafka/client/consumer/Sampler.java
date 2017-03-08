package org.embulk.input.kafka.client.consumer;

import java.io.UnsupportedEncodingException;

public interface Sampler {
  void sampling() throws UnsupportedEncodingException;
}
