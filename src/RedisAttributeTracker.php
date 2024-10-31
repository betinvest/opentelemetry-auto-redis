<?php

/**
 * @copyright  Copyright (c) 2024 E-vino Comércio de Vinhos S.A. (https://evino.com.br)
 * @author     Kevin Mian Kraiker <kevin.kraiker@evino.com.br>
 * @Link       https://evino.com.br
 */

declare(strict_types=1);

namespace OpenTelemetry\Contrib\Instrumentation\Redis;

use Credis_Client;
use OpenTelemetry\SemConv\TraceAttributes;
use OpenTelemetry\SemConv\TraceAttributeValues;
use Predis\Client;
use Predis\Connection\NodeConnectionInterface;
use Predis\Connection\ParametersInterface;
use Redis;
use RedisCluster;
use Throwable;
use WeakMap;

class RedisAttributeTracker
{
    /**
     * @var WeakMap<Redis|Client|Credis_Client|RedisCluster, iterable<non-empty-string, bool|int|float|string|array|null>>
     */
    private WeakMap $redisToAttributesMap;

    public function __construct()
    {
        /** @psalm-suppress PropertyTypeCoercion */
        $this->redisToAttributesMap = new WeakMap();
    }

    /**
     * @param Redis|Client|Credis_Client|RedisCluster $redis
     *
     * @return iterable<non-empty-string, bool|int|float|string|array|null>
     */
    public function trackRedisAttributes(Redis|Client|Credis_Client|RedisCluster $redis): iterable
    {
        /** @var array<non-empty-string, bool|int|float|string|array|null> $attributes */
        $attributes = $this->redisToAttributesMap[$redis]
            ?? [TraceAttributes::DB_SYSTEM => TraceAttributeValues::DB_SYSTEM_REDIS];

        try {
            if ($redis instanceof Redis) {
                if (!$host = $redis->getHost()) {
                    return $attributes;
                }
                $attributes[TraceAttributes::SERVER_ADDRESS] = $host;
                if (str_contains($host, '/') || str_starts_with($host, 'unix:')) {
                    $attributes[TraceAttributes::NETWORK_TRANSPORT] = 'unix';
                } else {
                    $attributes[TraceAttributes::SERVER_PORT] = $redis->getPort() ?: 6379;
                }
                if ($dbNum = $redis->getDbNum()) {
                    $attributes[TraceAttributes::DB_REDIS_DATABASE_INDEX] = $dbNum;
                }
                if (($auth = $redis->getAuth()) && is_array($auth) && count($auth) > 1) { // @phpstan-ignore-line
                    $attributes[TraceAttributes::DB_USER] = $auth[0];
                }
            }
            if ($redis instanceof Client) {
                $connection = $redis->getConnection();
                if ($connection instanceof NodeConnectionInterface) {
                    /** @var \stdClass|array|ParametersInterface $parameters */
                    $parameters = $connection->getParameters();
                    if (is_array($parameters)) {
                        $parameters = (object) $parameters;
                    }
                    $attributes[TraceAttributes::SERVER_ADDRESS] = $parameters->host ?? $parameters->path ?? 'unknown';
                    if (isset($parameters->port)) {
                        $attributes[TraceAttributes::SERVER_PORT] = $parameters->port;
                    }
                    if (isset($parameters->scheme)) {
                        $attributes[TraceAttributes::NETWORK_TRANSPORT] = strtolower($parameters->scheme);
                    }
                    if (isset($parameters->parameters->database)) {
                        $attributes[TraceAttributes::DB_REDIS_DATABASE_INDEX] = $parameters->parameters->database;
                    } elseif (is_array($parameters->parameters) &&
                        array_key_exists('database', $parameters->parameters)) {
                        $attributes[TraceAttributes::DB_REDIS_DATABASE_INDEX] = $parameters->parameters['database'];
                    }
                    if (isset($parameters->parameters->username)) {
                        $attributes[TraceAttributes::DB_USER] = $parameters->parameters->username;
                    } elseif (isset($parameters->username)) {
                        $attributes[TraceAttributes::DB_USER] = $parameters->username;
                    } elseif (is_array($parameters->parameters) &&
                        array_key_exists('username', $parameters->parameters)) {
                        $attributes[TraceAttributes::DB_USER] = $parameters->parameters['username'];
                    }
                }
            }
            if ($redis instanceof Credis_Client) {
                $attributes[TraceAttributes::SERVER_ADDRESS] = $host = $redis->getHost();
                if (str_contains($host, '/') || str_starts_with($host, 'unix:')) {
                    $attributes[TraceAttributes::NETWORK_TRANSPORT] = 'unix';
                } else {
                    $attributes[TraceAttributes::NETWORK_TRANSPORT] = 'tcp';
                    $attributes[TraceAttributes::SERVER_PORT] = $redis->getPort();
                }
                $attributes[TraceAttributes::DB_REDIS_DATABASE_INDEX] = $redis->getSelectedDb();
            }
        } catch (Throwable $e) {
            // if we catched an exception, the driver is likely not supporting the operation, default to "other"
            $attributes[TraceAttributes::DB_SYSTEM] = 'other_sql';
        }

        return $this->redisToAttributesMap[$redis] = $attributes;
    }

    public function trackRedisAuth(Redis|Client|Credis_Client|RedisCluster $redis, string $username): array
    {
        $attributes = $this->redisToAttributesMap[$redis] ?? null;
        if ($attributes === null) {
            return [];
        }
        $attributes[TraceAttributes::DB_USER] = $username;
        return $this->redisToAttributesMap[$redis] = $attributes;
    }

    public function trackRedisDbIdx(Redis|Client|Credis_Client|RedisCluster $redis, int $dbIndex): array
    {
        $attributes = $this->redisToAttributesMap[$redis] ?? null;
        if ($attributes === null) {
            return [];
        }
        $attributes[TraceAttributes::DB_REDIS_DATABASE_INDEX] = $dbIndex;
        return $this->redisToAttributesMap[$redis] = $attributes;
    }

    public function trackedAttributesForRedis(Redis|Client|Credis_Client|RedisCluster $redis)
    {
        return $this->redisToAttributesMap[$redis] ?? [];
    }
}
