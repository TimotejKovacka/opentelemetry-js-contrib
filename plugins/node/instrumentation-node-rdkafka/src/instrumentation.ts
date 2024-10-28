/*
 * Copyright The OpenTelemetry Authors,
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import {
  SpanKind,
  Span,
  SpanStatusCode,
  Context,
  propagation,
  Link,
  trace,
  context,
  ROOT_CONTEXT,
} from '@opentelemetry/api';
import {
  MESSAGINGOPERATIONVALUES_PROCESS,
  MESSAGINGOPERATIONVALUES_RECEIVE,
  SEMATTRS_MESSAGING_SYSTEM,
  SEMATTRS_MESSAGING_DESTINATION,
  SEMATTRS_MESSAGING_OPERATION,
} from '@opentelemetry/semantic-conventions';
import type * as rdkafka from 'node-rdkafka';
import type {
  EachBatchHandler,
  EachMessageHandler,
  Producer,
  RecordMetadata,
  Message,
  ConsumerRunConfig,
  KafkaMessage,
  Consumer,
} from 'kafkajs';
import {
  KafkaJsInstrumentationConfig,
  NodeRdKafkaInstrumentationConfig,
} from './types';
import { PACKAGE_NAME, PACKAGE_VERSION } from './version';
import { bufferTextMapGetter } from './propagator';
import {
  InstrumentationBase,
  InstrumentationNodeModuleDefinition,
  safeExecuteInTheMiddle,
  isWrapped,
} from '@opentelemetry/instrumentation';
import {
  KafkaConsumer,
  LibrdKafkaError,
  MessageHeader,
  MessageKey,
  MessageValue,
  NumberNullUndefined,
} from 'node-rdkafka';

export class NodeRdkafkaInstrumentation extends InstrumentationBase<NodeRdKafkaInstrumentationConfig> {
  constructor(config: NodeRdKafkaInstrumentationConfig = {}) {
    super(PACKAGE_NAME, PACKAGE_VERSION, config);
  }

  protected init() {
    const module = new InstrumentationNodeModuleDefinition(
      'node-rdkafka',
      ['>=0.1.0'],
      this._patch.bind(this),
      this._unpatch.bind(this)
    );
    return module;
  }

  private _patch(moduleExports: typeof rdkafka) {
    this._wrap(
      moduleExports.Producer.prototype,
      'produce',
      this._getProducePatch()
    );
    this._wrap(
      moduleExports.KafkaConsumer.prototype,
      'consume',
      this._getConsumePatch()
    );
    this._wrap(
      moduleExports.KafkaConsumer.prototype,
      'subscribe',
      this._getSubscribePatch()
    );
    return moduleExports;
  }

  private _unpatch(moduleExports: typeof rdkafka) {
    if (isWrapped(moduleExports?.Producer?.prototype?.produce)) {
      this._unwrap(moduleExports.Producer.prototype, 'produce');
    }
    if (isWrapped(moduleExports?.KafkaConsumer?.prototype?.consume)) {
      this._unwrap(moduleExports.KafkaConsumer.prototype, 'consume');
    }
    if (isWrapped(moduleExports?.KafkaConsumer?.prototype?.subscribe)) {
      this._unwrap(moduleExports.KafkaConsumer.prototype, 'subscribe');
    }
    return moduleExports;
  }

  private _getProducePatch() {
    const instrumentation = this;
    return (original: typeof rdkafka.Producer.prototype.produce) => {
      return function produce(
        this: Producer,
        topic: string,
        partition: NumberNullUndefined,
        message: MessageValue,
        key?: MessageKey,
        timestamp?: NumberNullUndefined,
        opaque?: any,
        headers?: MessageHeader[]
      ) {
        const messageHeaders = headers || [];
        const activeContext = context.active();

        const span = instrumentation._startProducerSpan(topic, {
          message:
            message instanceof Buffer ? message : Buffer.from(message || ''),
          key,
          headers: messageHeaders,
        });

        const withSpanContext = trace.setSpan(activeContext, span);

        try {
          const headerAdapter: { [key: string]: string } = {};
          propagation.inject(withSpanContext, headerAdapter);
          for (const [key, value] of Object.entries(headerAdapter)) {
            messageHeaders.push({ [key]: value });
          }

          const result = original.call(
            this,
            topic,
            partition,
            message,
            key,
            timestamp,
            opaque,
            messageHeaders
          );

          span.setAttribute(
            SEMATTRS_MESSAGING_OPERATION,
            MESSAGINGOPERATIONVALUES_PROCESS
          );

          return result;
        } catch (error) {
          span.recordException(error as any);
          span.setStatus({
            code: SpanStatusCode.ERROR,
            message:
              error instanceof Error ? error.message : 'Unknown producer error',
          });
          throw error;
        } finally {
          span.end();
        }
      };
    };
  }

  private _startProducerSpan(
    topic: string,
    message: {
      message: Buffer;
      key?: MessageKey;
      headers?: MessageHeader[];
    }
  ): Span {
    const span = this.tracer.startSpan(topic, {
      kind: SpanKind.PRODUCER,
      attributes: {
        [SEMATTRS_MESSAGING_SYSTEM]: 'kafka',
        [SEMATTRS_MESSAGING_DESTINATION]: topic,
      },
    });

    const { producerHook } = this.getConfig();
    if (producerHook) {
      safeExecuteInTheMiddle(
        () => producerHook(span, { topic, message }),
        e => {
          if (e) this._diag.error('producerHook error', e);
        },
        true
      );
    }

    return span;
  }

  private _getConsumePatch() {
    return (original: typeof rdkafka.KafkaConsumer.prototype.consume) => {
      return function consume(
        this: KafkaConsumer,
        ...args:
          | [number, ((err: LibrdKafkaError, messages: Message[]) => void)?]
          | [(err: LibrdKafkaError, messages: Message[]) => void]
          | []
      ) {
        if (args.length === 0) {
          return original.call(this);
        }

        if (args.length === 1 && typeof args[0] === 'function') {
          // consume(callback)
          const callback = args[0];
          return original.call(
            this,
            (err: LibrdKafkaError, messages: rdkafka.Message[]) => {
              if (err) {
                callback(err, []);
                return;
              }

              try {
                const processedMessages = messages.map(message =>
                  instrumentation._processConsumedMessage(message, this)
                );
                callback(null as any, processedMessages);
              } catch (error) {
                callback(error as LibrdKafkaError, []);
              }
            }
          );
        }
      };
    };
  }
}
