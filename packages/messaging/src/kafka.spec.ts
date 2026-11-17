import { ALL_TOPICS, dlqTopic, KafkaClient, KafkaProducer } from './kafka';
import { testConfig } from './testing';

function clientWithAdmin(
  existing: string[],
  created = true,
): { client: KafkaClient; createTopics: jest.Mock } {
  const createTopics = jest.fn().mockResolvedValue(created);
  const admin = {
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    listTopics: jest.fn().mockResolvedValue(existing),
    createTopics,
  };
  const client = new KafkaClient(testConfig());
  Object.assign(client, { kafka: { admin: () => admin } });
  return { client, createTopics };
}

describe('topics', () => {
  it('has a dead-letter twin for every topic', () => {
    expect(dlqTopic('orders')).toBe('orders.dlq');
    expect(ALL_TOPICS).toEqual([
      'orders',
      'orders.dlq',
      'inventory',
      'inventory.dlq',
      'payments',
      'payments.dlq',
    ]);
  });
});

describe('KafkaClient.ensureTopics', () => {
  it('creates only the missing topics, with 3 partitions each', async () => {
    const { client, createTopics } = clientWithAdmin(['orders', 'orders.dlq']);
    await client.ensureTopics();
    const [[arg]] = createTopics.mock.calls as [[{ topics: { topic: string; numPartitions: number }[] }]];
    expect(arg.topics.map((t) => t.topic)).toEqual([
      'inventory',
      'inventory.dlq',
      'payments',
      'payments.dlq',
    ]);
    expect(arg.topics.every((t) => t.numPartitions === 3)).toBe(true);
  });

  it('does not call createTopics when everything exists', async () => {
    const { client, createTopics } = clientWithAdmin([...ALL_TOPICS]);
    await client.ensureTopics();
    expect(createTopics).not.toHaveBeenCalled();
  });

  it('is fine when a peer created the topics first', async () => {
    const { client } = clientWithAdmin([], false);
    await expect(client.ensureTopics()).resolves.toBeUndefined();
  });
});

describe('KafkaProducer', () => {
  it('ensures topics before connecting, and disconnects on shutdown', async () => {
    const calls: string[] = [];
    const producer = {
      connect: jest.fn(() => calls.push('connect')),
      disconnect: jest.fn(() => calls.push('disconnect')),
    };
    const client = {
      kafka: { producer: () => producer },
      ensureTopics: jest.fn(() => calls.push('ensureTopics')),
    } as unknown as KafkaClient;
    const kafkaProducer = new KafkaProducer(client);
    await kafkaProducer.onModuleInit();
    await kafkaProducer.beforeApplicationShutdown();
    expect(calls).toEqual(['ensureTopics', 'connect', 'disconnect']);
  });
});
