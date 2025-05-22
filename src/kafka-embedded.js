'use strict';

const { spawn, exec } = require('child_process');
const path = require('path');
const StringDecoder = require('string_decoder').StringDecoder;
require('fs');
const tmp = require('tmp');
const portfinder = require('portfinder');
const createEditor = require('properties-parser').createEditor;

// Configuration constants
const CONFIG = {
  BASE_PORT: 18000,
  KAFKA_LOG_ENV: 'KAFKA_PLEASE_LOG',
  WAIT_TIME: 10000,
  IS_WINDOWS: process.platform === 'win32',
  PATHS: {
    KAFKA_PROPERTIES: path.join(__dirname, '..', 'kafka', 'config', 'server.properties'),
    KAFKA_BIN: path.join(__dirname, '..', 'kafka', 'bin'),
    LOG4J_PROPS: path.join(__dirname, 'log4j-stdout.properties')
  }
};

class KafkaManager {
  constructor() {
    this.decoder = new StringDecoder('utf8');
    this.takenPorts = [];
    tmp.setGracefulCleanup();
    portfinder.basePort = CONFIG.BASE_PORT;
    this.setupConsoleOverrides();
  }

  setupConsoleOverrides() {
    const customConsoleOutput = (message) => {
      if (typeof message === 'string') {
        try {
          const parsed = JSON.parse(message);
          process.stdout.write(JSON.stringify(parsed) + '\n');
        } catch (e) {
          process.stdout.write(message + '\n');
        }
      } else {
        process.stdout.write(JSON.stringify(message) + '\n');
      }
    };

    console.error = console.warn = console.info = customConsoleOutput;
  }

  logDebug(msg, args) {
    if (process.env[CONFIG.KAFKA_LOG_ENV] === 'verbose') {
      console.log(msg, args);
    }
  }

  async waitFor(ms) {
    this.logDebug(`Now waiting for ${ms} ms.`);
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  async getAvailablePort() {
    return new Promise((resolve, reject) => {
      const getPortRecursive = (callback) => {
        portfinder.getPort((err, port) => {
          if (err) return callback(err);
          if (this.takenPorts.includes(port)) {
            getPortRecursive(callback);
          } else {
            callback(null, port);
          }
        });
      };

      getPortRecursive((err, port) => {
        if (err) reject(err);
        else {
          this.logDebug('available port', port);
          resolve(port);
        }
      });
    });
  }

  async allocatePorts() {
    return new Promise((resolve, reject) => {
      portfinder.getPorts(2, {}, (err, ports) => {
        if (err) return reject(err);
        resolve({ kafkaPort: ports[0], controllerPort: ports[1] });
      });
    });
  }

  async createTempDirectory(prefix) {
    return new Promise((resolve, reject) => {
      tmp.dir({ prefix, unsafeCleanup: true }, (err, path) => {
        err ? reject(err) : resolve(path);
      });
    });
  }

  async initializeStorage(propertiesFile) {
    const scriptPath = CONFIG.IS_WINDOWS
      ? path.join(CONFIG.PATHS.KAFKA_BIN, 'windows', 'kafka-storage.bat')
      : path.join(CONFIG.PATHS.KAFKA_BIN, 'kafka-storage.sh');

    const command = `${scriptPath} format -t "$(${scriptPath} random-uuid)" -c ${propertiesFile} --standalone`;

    return new Promise((resolve, reject) => {
      exec(command, (error, stdout, stderr) => {
        if (error) {
          reject(error);
          return;
        }
        if (stderr) {
          this.logDebug(`stderr: ${stderr}`);
        }
        this.logDebug(`UUID: ${stdout.trim()}`);
        resolve();
      });
    });
  }

  async createKafkaConfig({ kafkaDir, controllerPort, kafkaPort }) {
    this.logDebug('Making kafka config file', kafkaDir);
    return new Promise((resolve, reject) => {
      createEditor(CONFIG.PATHS.KAFKA_PROPERTIES, {}, (err, props) => {
        if (err) return reject(err);

        const logDir = kafkaDir.replace(/\\/g, '\\\\');
        props.set('log.dirs', logDir);
        props.set('controller.quorum.bootstrap.servers', `localhost:${controllerPort}`);
        props.set('listeners', `PLAINTEXT://localhost:${kafkaPort},CONTROLLER://localhost:${controllerPort}`);
        props.set('advertised.listeners', `PLAINTEXT://localhost:${kafkaPort},CONTROLLER://localhost:${controllerPort}`);
        props.set('controller.listener.names', 'CONTROLLER');

        const finalName = path.join(kafkaDir, 'server.properties');
        props.save(finalName, () => resolve(finalName));
      });
    });
  }

  async killProcess(proc) {
    return new Promise((resolve) => {
      proc.on('exit', (code) => {
        this.logDebug(`The process exited. Code ${code}. Resolve promise.`);
        resolve();
      });
      proc.kill('SIGKILL');
    });
  }

  async stopKafkaServer() {
    this.logDebug('Stopping kafka...');
    return new Promise((resolve) => {
      const scriptPath = CONFIG.IS_WINDOWS
        ? path.join(CONFIG.PATHS.KAFKA_BIN, 'windows', 'kafka-server-stop.bat')
        : path.join(CONFIG.PATHS.KAFKA_BIN, 'kafka-server-stop.sh');

      const proc = spawn(scriptPath);
      proc.on('exit', resolve);
    });
  }

  async startKafkaProcess({ kafkaDir, kafkaPort }) {
    const scriptPath = CONFIG.IS_WINDOWS
      ? path.join(CONFIG.PATHS.KAFKA_BIN, 'windows', 'kafka-server-start.bat')
      : path.join(CONFIG.PATHS.KAFKA_BIN, 'kafka-server-start.sh');

    const propertiesFile = path.join(kafkaDir, 'server.properties');

    this.logDebug('starting kafka, config', propertiesFile);

    return new Promise((resolve) => {
      // Using kafkaProcess instead of process to avoid naming conflict
      const kafkaProcess = spawn(scriptPath, [propertiesFile], {
        env: { ...process.env, JMX_PORT: '' },
        cwd: kafkaDir
      });

      kafkaProcess.stdout.on('data', (data) => {
        const message = this.decoder.write(data);
        this.logDebug('Kafka output:', message);
      });

      kafkaProcess.stderr.on('data', (data) => {
        this.logDebug('Kafka error:', this.decoder.write(data));
      });

      kafkaProcess.on('error', (error) => {
        this.logDebug('Failed to start Kafka process', error);
      });

      kafkaProcess.on('exit', (code) => {
        this.logDebug(`Child exited with code ${code}`);
      });

      this.logDebug('Now starting with the health check stuff...');

      // Wait for Kafka to start
      setTimeout(() => {
        this.logDebug('Kafka startup time elapsed');
        resolve({
          kafkaPort,
          close: () => this.killProcess(kafkaProcess).then(() => this.stopKafkaServer())
        });
      }, CONFIG.WAIT_TIME);
    });
  }

  async startKafkaServer() {
    try {
      const ports = await this.allocatePorts();
      const kafkaDir = await this.createTempDirectory('kafka-');
      const propertiesFile = await this.createKafkaConfig({
        kafkaDir,
        controllerPort: ports.controllerPort,
        kafkaPort: ports.kafkaPort
      });

      await this.initializeStorage(propertiesFile);
      return await this.startKafkaProcess({ kafkaDir, kafkaPort: ports.kafkaPort });
    } catch (error) {
      this.logDebug('Error starting Kafka server:', error);
      throw error;
    }
  }
}

module.exports = function makeKafkaServer() {
  return new KafkaManager().startKafkaServer();
};