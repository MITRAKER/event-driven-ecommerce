import { InvalidEventError, InvariantError } from '@shop/contracts';
import { classifyFailure } from './errors';

function withCode(code: string, message = 'boom'): Error {
  return Object.assign(new Error(message), { code });
}

describe('classifyFailure', () => {
  it('treats bad messages and broken invariants as permanent', () => {
    expect(classifyFailure(new InvalidEventError('bad'))).toBe('permanent');
    expect(classifyFailure(new InvariantError('stock < 0'))).toBe('permanent');
  });

  it.each([
    ['08006', 'connection failure'],
    ['57P01', 'admin shutdown'],
    ['57014', 'statement timeout'],
    ['53300', 'too many connections'],
    ['40P01', 'deadlock detected'],
    ['40001', 'serialization failure'],
    ['ECONNREFUSED', 'network'],
  ])('treats %s (%s) as infrastructure', (code) => {
    expect(classifyFailure(withCode(code))).toBe('infrastructure');
  });

  it('recognises pg-pool connection messages that carry no code', () => {
    expect(classifyFailure(new Error('timeout exceeded when trying to connect'))).toBe('infrastructure');
    expect(classifyFailure(new Error('Connection terminated unexpectedly'))).toBe('infrastructure');
  });

  it('treats everything else as unknown', () => {
    expect(classifyFailure(new TypeError('x is undefined'))).toBe('unknown');
    expect(classifyFailure(withCode('23505', 'duplicate key'))).toBe('unknown');
    expect(classifyFailure('a string')).toBe('unknown');
  });
});
