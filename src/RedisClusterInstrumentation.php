<?php

declare(strict_types=1);

namespace OpenTelemetry\Contrib\Instrumentation\Redis;

use OpenTelemetry\API\Instrumentation\CachedInstrumentation;
use OpenTelemetry\API\Trace\Span;
use OpenTelemetry\API\Trace\SpanBuilderInterface;
use OpenTelemetry\API\Trace\SpanKind;
use OpenTelemetry\API\Trace\StatusCode;
use OpenTelemetry\Context\Context;
use OpenTelemetry\SemConv\TraceAttributes;
use Throwable;

use function OpenTelemetry\Instrumentation\hook;

/**
 * @SuppressWarnings(PHPMD.StaticAccess)
 */
class RedisClusterInstrumentation
{
    public const NAME = 'redis';

    public static function register(): void
    {
        $instrumentation = new CachedInstrumentation('io.opentelemetry.contrib.php.redis');
        $attributeTracker = new RedisAttributeTracker();

        $genericPostHook = static function (\RedisCluster $redis, array $params, mixed $ret, ?Throwable $exception) {
            self::end($exception);
        };

        hook(
            \RedisCluster::class,
            '__construct',
            pre: static function (
                \RedisCluster $redis,
                array $params,
                string $class,
                string $function,
                ?string $filename,
                ?int $lineno,
            ) use ($instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder(
                    $instrumentation,
                    'RedisCluster::__construct',
                    $function,
                    $class,
                    $filename,
                    $lineno,
                )
                    ->setSpanKind(SpanKind::KIND_CLIENT);
                if (isset($params[0]) && is_array($params[0]) && array_key_exists('host', $params[0])) {
                    $options = $params[0];
                    $builder->setAttribute(TraceAttributes::SERVER_ADDRESS, $options['host']);
                    if (!str_starts_with($options['host'], 'unix:') && !str_contains($options['host'], '/')) {
                        $builder->setAttribute(TraceAttributes::NETWORK_TRANSPORT, 'tcp');
                        $builder->setAttribute(TraceAttributes::SERVER_PORT, $options['port'] ?? 6379);
                    } else {
                        $builder->setAttribute(TraceAttributes::NETWORK_TRANSPORT, 'unix');
                    }
                    if (array_key_exists('auth', $options) && is_array($auth = $options['auth']) && count($auth) > 1) {
                        $builder->setAttribute(TraceAttributes::DB_USER, $auth[0]);
                    }
                }
                $parent = Context::getCurrent();
                $span = $builder->startSpan();
                Context::storage()->attach($span->storeInContext($parent));
            },
            post: static function (\RedisCluster $redis, array $params, mixed $ret, ?Throwable $exception) use (
                $attributeTracker,
            ) {
                $scope = Context::storage()->scope();
                if (!$scope) {
                    return;
                }
                $span = Span::fromContext($scope->context());

                $attributes = $attributeTracker->trackRedisAttributes($redis);
                $span->setAttributes($attributes);

                self::end($exception);
            },
        );
        hook(
            \RedisCluster::class,
            'exists',
            pre: self::generateVarargsPreHook('exists', $instrumentation, $attributeTracker),
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'get',
            pre: static function (
                \RedisCluster $redis,
                array $params,
                string $class,
                string $function,
                ?string $filename,
                ?int $lineno,
            ) use ($attributeTracker, $instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'RedisCluster::get', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT);
                if ($class === \RedisCluster::class) {
                    $builder->setAttribute(
                        TraceAttributes::DB_STATEMENT,
                        isset($params[0]) ? 'GET ' . $params[0] : 'undefined',
                    );
                }
                $parent = Context::getCurrent();
                $span = $builder->startSpan();

                $attributes = $attributeTracker->trackedAttributesForRedis($redis);
                $span->setAttributes($attributes);

                Context::storage()->attach($span->storeInContext($parent));
            },
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'mget',
            pre: self::generateVarargsPreHook('mGet', $instrumentation, $attributeTracker),
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'set',
            pre: static function (
                \RedisCluster $redis,
                array $params,
                string $class,
                string $function,
                ?string $filename,
                ?int $lineno,
            ) use ($attributeTracker, $instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'RedisCluster::set', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT);
                if ($class === \RedisCluster::class) {
                    $statement = 'SET ' . $params[0] . ' ?';
                    if (isset($params[2]) && is_array($params[2])) {
                        foreach ($params[2] as $key => $value) {
                            $statement .= ' ' . strtoupper((string) $key) . ' ' . $value;
                        }
                    }
                    $builder->setAttribute(
                        TraceAttributes::DB_STATEMENT,
                        $statement,
                    );
                }
                $parent = Context::getCurrent();
                $span = $builder->startSpan();

                $attributes = $attributeTracker->trackedAttributesForRedis($redis);
                $span->setAttributes($attributes);

                Context::storage()->attach($span->storeInContext($parent));
            },
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'setex',
            pre: static function (
                \RedisCluster $redis,
                array $params,
                string $class,
                string $function,
                ?string $filename,
                ?int $lineno,
            ) use ($attributeTracker, $instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'RedisCluster::setEx', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT);
                if ($class === \RedisCluster::class) {
                    $statement = 'SETEX ' . $params[0] . ' ' . $params[1] . ' ?';
                    $builder->setAttribute(
                        TraceAttributes::DB_STATEMENT,
                        $statement,
                    );
                }
                $parent = Context::getCurrent();
                $span = $builder->startSpan();

                $attributes = $attributeTracker->trackedAttributesForRedis($redis);
                $span->setAttributes($attributes);

                Context::storage()->attach($span->storeInContext($parent));
            },
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'scan',
            pre: static function (
                \RedisCluster $redis,
                array $params,
                string $class,
                string $function,
                ?string $filename,
                ?int $lineno,
            ) use ($attributeTracker, $instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'RedisCluster::scan', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT);
                if ($class === \RedisCluster::class) {
                    $statement = 'SCAN ' . $params[0];
                    if (!empty($params[1])) {
                        $statement .= ' MATCH ' . $params[1];
                    }
                    if (!empty($params[2])) {
                        $statement .= ' COUNT ' . $params[2];
                    }
                    if (!empty($params[3])) {
                        $statement .= ' TYPE ' . $params[3];
                    }
                    $builder->setAttribute(
                        TraceAttributes::DB_STATEMENT,
                        $statement,
                    );
                }
                $parent = Context::getCurrent();
                $span = $builder->startSpan();

                $attributes = $attributeTracker->trackedAttributesForRedis($redis);
                $span->setAttributes($attributes);

                Context::storage()->attach($span->storeInContext($parent));
            },
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'del',
            pre: self::generateVarargsPreHook('del', $instrumentation, $attributeTracker),
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'sAdd',
            pre: static function (
                \RedisCluster $redis,
                array $params,
                string $class,
                string $function,
                ?string $filename,
                ?int $lineno,
            ) use ($attributeTracker, $instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'RedisCluster::sAdd', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT);
                if ($class === \RedisCluster::class) {
                    $statement = 'SADD';
                    $maskValues = false;
                    foreach ($params as $value) {
                        if ($maskValues) {
                            $statement .= ' ?';

                            continue;
                        }
                        $maskValues = true;
                        $statement .= " $value";
                    }
                    $builder->setAttribute(
                        TraceAttributes::DB_STATEMENT,
                        $statement,
                    );
                }
                $parent = Context::getCurrent();
                $span = $builder->startSpan();

                $attributes = $attributeTracker->trackedAttributesForRedis($redis);
                $span->setAttributes($attributes);

                Context::storage()->attach($span->storeInContext($parent));
            },
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'sRem',
            pre: static function (
                \RedisCluster $redis,
                array $params,
                string $class,
                string $function,
                ?string $filename,
                ?int $lineno,
            ) use ($attributeTracker, $instrumentation) {
                /** @psalm-suppress ArgumentTypeCoercion */
                $builder = self::makeBuilder($instrumentation, 'RedisCluster::sRem', $function, $class, $filename, $lineno)
                    ->setSpanKind(SpanKind::KIND_CLIENT);
                if ($class === \RedisCluster::class) {
                    $statement = 'SREM';
                    $maskValues = false;
                    foreach ($params as $value) {
                        if ($maskValues) {
                            $statement .= ' ?';

                            continue;
                        }
                        $maskValues = true;
                        $statement .= " $value";
                    }
                    $builder->setAttribute(
                        TraceAttributes::DB_STATEMENT,
                        $statement,
                    );
                }
                $parent = Context::getCurrent();
                $span = $builder->startSpan();

                $attributes = $attributeTracker->trackedAttributesForRedis($redis);
                $span->setAttributes($attributes);

                Context::storage()->attach($span->storeInContext($parent));
            },
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'multi',
            pre: self::generateSimplePreHook('multi', $instrumentation, $attributeTracker),
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'exec',
            pre: self::generateSimplePreHook('exec', $instrumentation, $attributeTracker),
            post: $genericPostHook,
        );
        hook(
            \RedisCluster::class,
            'multi',
            pre: self::generateSimplePreHook('discard', $instrumentation, $attributeTracker),
            post: $genericPostHook,
        );
    }

    private static function makeBuilder(
        CachedInstrumentation $instrumentation,
        string $name,
        string $function,
        string $class,
        ?string $filename,
        ?int $lineno,
    ): SpanBuilderInterface {
        /** @psalm-suppress ArgumentTypeCoercion */
        return $instrumentation->tracer()
            ->spanBuilder($name)
            ->setAttribute(TraceAttributes::CODE_FUNCTION, $function)
            ->setAttribute(TraceAttributes::CODE_NAMESPACE, $class)
            ->setAttribute(TraceAttributes::CODE_FILEPATH, $filename)
            ->setAttribute(TraceAttributes::CODE_LINENO, $lineno);
    }

    private static function generateSimplePreHook(
        string $command,
        CachedInstrumentation $instrumentation,
        RedisAttributeTracker $attributeTracker,
    ): callable {
        return static function (
            \RedisCluster $redis,
            array $params,
            string $class,
            string $function,
            ?string $filename,
            ?int $lineno,
        ) use ($attributeTracker, $instrumentation, $command) {
            /** @psalm-suppress ArgumentTypeCoercion */
            $builder = self::makeBuilder($instrumentation, "RedisCluster::$command", $function, $class, $filename, $lineno)
                ->setSpanKind(SpanKind::KIND_CLIENT);
            $parent = Context::getCurrent();
            $span = $builder->startSpan();
            $attributes = $attributeTracker->trackedAttributesForRedis($redis);
            $span->setAttributes($attributes);
            Context::storage()->attach($span->storeInContext($parent));
        };
    }

    private static function generateVarargsPreHook(
        string $command,
        CachedInstrumentation $instrumentation,
        RedisAttributeTracker $attributeTracker,
    ): callable {
        return static function (
            \RedisCluster $redis,
            array $params,
            string $class,
            string $function,
            ?string $filename,
            ?int $lineno,
        ) use ($attributeTracker, $instrumentation, $command) {
            /** @psalm-suppress ArgumentTypeCoercion */
            $builder = self::makeBuilder($instrumentation, "RedisCluster::$command", $function, $class, $filename, $lineno)
                ->setSpanKind(SpanKind::KIND_CLIENT);
            if ($class === \RedisCluster::class) {
                if (isset($params[0]) && is_array($params[0])) {
                    $params = $params[0];
                }
                $builder->setAttribute(
                    TraceAttributes::DB_STATEMENT,
                    isset($params[0]) ? strtoupper($command) . ' ' . (implode(' ', $params)) : 'undefined',
                );
            }
            $parent = Context::getCurrent();
            $span = $builder->startSpan();

            $attributes = $attributeTracker->trackedAttributesForRedis($redis);
            $span->setAttributes($attributes);

            Context::storage()->attach($span->storeInContext($parent));
        };
    }

    private static function end(?Throwable $exception): void
    {
        $scope = Context::storage()->scope();
        if (!$scope) {
            return;
        }
        $scope->detach();
        $span = Span::fromContext($scope->context());
        if ($exception) {
            $span->recordException($exception, [TraceAttributes::EXCEPTION_ESCAPED => true]);
            $span->setStatus(StatusCode::STATUS_ERROR, $exception->getMessage());
        }

        $span->end();
    }
}
