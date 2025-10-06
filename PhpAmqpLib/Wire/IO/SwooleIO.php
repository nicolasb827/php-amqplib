<?php

namespace PhpAmqpLib\Wire\IO;

use PhpAmqpLib\Exception\AMQPRuntimeException;

// use PhpAmqpLib\Wire\AMQPWriter;

class SwooleIO extends AbstractIO
{
    /** @var null|int */
    protected $crypto_method;

    /** @var \OpenSwoole\Coroutine\Client */
    private $sock;

    private $buffer;
    /**
     * @param string $host
     * @param int $port
     * @param float $read_write_timeout
     * @param resource|null $context
     * @param bool $keepalive
     * @param int $heartbeat
     * @param string|null $ssl_protocol @deprecated
     */
    public function __construct(
        $host,
        $port,
        $read_timeout,
        $keepalive = false,
        $write_timeout,
        $heartbeat = 0,
        $ssl_protocol = null
    )
    {
        if (func_num_args() === 8) {
            trigger_error(
                '$ssl_protocol parameter is deprecated, use stream_context_set_option($context, \'ssl\', \'crypto_method\', $ssl_protocol) instead (see https://www.php.net/manual/en/function.stream-socket-enable-crypto.php for possible values)',
                E_USER_DEPRECATED
            );
        }
        // TODO FUTURE change comparison to <=
        // php-amqplib/php-amqplib#648, php-amqplib/php-amqplib#666
        /*
            TODO FUTURE enable this check
        if ($heartbeat !== 0 && ($read_write_timeout < ($heartbeat * 2))) {
            throw new \InvalidArgumentException('read_write_timeout must be at least 2x the heartbeat');
        }
         */

        $this->host = $host;
        $this->port = $port;
        $this->read_timeout = (float)$read_timeout;
        $this->write_timeout = (float)$write_timeout;
        $this->keepalive = $keepalive;
        $this->heartbeat = $heartbeat;
        $this->initial_heartbeat = $heartbeat;
        $this->canDispatchPcntlSignal = $this->isPcntlSignalEnabled();
        if (!is_null($ssl_protocol)) {
            $this->crypto_method = $ssl_protocol;
        }
    }

    /**
     * @inheritdoc
     */
    public function connect()
    {
        $sock = new \OpenSwoole\Coroutine\Client(\Openswoole\Constant::SOCK_TCP);
        if (!$sock->connect($this->host, $this->port, max($this->read_timeout, $this->write_timeout))) {
            throw new AMQPRuntimeException(
                sprintf(
                    'Error Connecting to server(%s): %s ',
                    $sock->errCode,
                    swoole_strerror($sock->errCode)
                ),
                $sock->errCode
            );
        }
        $this->sock = $sock;
        if (isset($this->crypto_method)) {
            $this->sock->enableSSL();
        }
    }

    /**
     * Reconnects the socket
     */
    public function reconnect()
    {
        $this->close();
        $this->connect();
    }

    /**
     * @inheritdoc
     */
    public function read($len)
    {
        $this->check_heartbeat();

        do {
            if ($len <= strlen($this->buffer)) {
                $data = substr($this->buffer, 0, $len);
                $this->buffer = substr($this->buffer, $len);
                $this->last_read = microtime(true);

                return $data;
            }

            if (!$this->sock->connected) {
                throw new AMQPRuntimeException('Broken pipe or closed connection');
            }

            $read_buffer = $this->sock->recv($this->read_timeout ? $this->read_timeout : -1);
            if ($read_buffer === false) {
                throw new AMQPRuntimeException('Error receiving data, errno=' . $this->sock->errCode);
            }

            if ($read_buffer === '') {
                continue;
            }

            $this->buffer .= $read_buffer;

        } while (true);


        return false;
    }

    /**
     * @inheritdoc
     */
    public function write($data)
    {
        $buffer = $this->sock->send($data, $this->write_timeout ? $this->write_timeout : -1);

        if ($buffer === false) {
            throw new AMQPRuntimeException('Error sending data: ' . $this->sock->errMsg);
        }

        if ($buffer === 0 && !$this->sock->connected) {
            throw new AMQPRuntimeException('Broken pipe or closed connection');
        }

        $this->last_write = microtime(true);
    }

    /**
     * Heartbeat logic: check connection health here
     */
    public function check_heartbeat()
    {
        // ignore unless heartbeat interval is set
        if ($this->heartbeat !== 0 && $this->last_read && $this->last_write) {
            $t = microtime(true);
            $t_read = round((float)($t - $this->last_read));
            $t_write = round((float)($t - $this->last_write));

            // server has gone away
            if (($this->heartbeat * 2) < $t_read) {
                $this->reconnect();
            }

            // time for client to send a heartbeat
            if (($this->heartbeat / 2) < $t_write) {
                $this->write_heartbeat();
            }
        }
    }

    /**
     * Sends a heartbeat message
     */
    protected function write_heartbeat()
    {
        $pkt = new AMQPWriter();
        $pkt->write_octet(8);
        $pkt->write_short(0);
        $pkt->write_long(0);
        $pkt->write_octet(0xCE);
        $this->write($pkt->getvalue());
    }

    public function close()
    {
        $this->disableHeartbeat();
        $this->sock->close();
        $this->sock = null;
        $this->last_read = 0;
        $this->last_write = 0;
    }

    /**
     * @deprecated
     * @return null|resource|\OpenSwoole\Coroutine\Client
     */
    public function getSocket()
    {
        return $this->sock;
    }

    /**
     * @inheritdoc
     */
    public function do_select(?int $sec, int $usec)
    {
        //var_dump($sec, $usec);
        $this->check_heartbeat();

        return 1;
    }


    /**
     * @return $this
     */
    public function disableHeartbeat()
    {
        $this->heartbeat = 0;

        return $this;
    }

    /**
     * @return $this
     */
    public function reenableHeartbeat()
    {
        $this->heartbeat = $this->initial_heartbeat;

        return $this;
    }
}
