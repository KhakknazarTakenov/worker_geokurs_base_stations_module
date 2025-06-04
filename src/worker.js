// worker.js
import Client from 'ssh2-sftp-client';
import { Client as SSHClient } from 'ssh2';
import Queue from 'bull';
import { logMessage } from './logger.js';
import path from 'path';
import express from 'express';
import './global.js'
import * as dotenv from 'dotenv';

const envPath = path.join(process.cwd(), '.env');
dotenv.config({ path: envPath });

// Настраиваем HTTP-сервер для мониторинга
const app = express();
app.get('/health', async (req, res) => {
    try {
        // Получаем статистику по задачам в очередях
        const queueStats = await Promise.all([
            Promise.all([
                usersQueue.getWaitingCount(),
                usersQueue.getActiveCount(),
                usersQueue.getCompletedCount(),
                usersQueue.getFailedCount()
            ]).then(([waiting, active, completed, failed]) => ({
                waiting, active, completed, failed
            })),
            Promise.all([
                groupsQueue.getWaitingCount(),
                groupsQueue.getActiveCount(),
                groupsQueue.getCompletedCount(),
                groupsQueue.getFailedCount()
            ]).then(([waiting, active, completed, failed]) => ({
                waiting, active, completed, failed
            })),
            Promise.all([
                mountsQueue.getWaitingCount(),
                mountsQueue.getActiveCount(),
                mountsQueue.getCompletedCount(),
                mountsQueue.getFailedCount()
            ]).then(([waiting, active, completed, failed]) => ({
                waiting, active, completed, failed
            }))
        ]);

        res.json({
            status: 'healthy',
            queues: {
                users: queueStats[0],
                groups: queueStats[1],
                mounts: queueStats[2]
            }
        });
    } catch (error) {
        await logMessage(LOG_TYPES.E, 'worker', `Health check failed: ${error.message}`);
        res.status(500).json({ status: 'error', error: error.message });
    }
});
app.listen(5473, () => console.log('Worker HTTP server running on port 5473'));

// Настраиваем очереди
const usersQueue = new Queue('usersQueue', {
    redis: { host: process.env.REDIS_HOST || 'localhost', port: process.env.REDIS_PORT || 6379 }
});
const groupsQueue = new Queue('groupsQueue', {
    redis: { host: process.env.REDIS_HOST || 'localhost', port: process.env.REDIS_PORT || 6379 }
});
const mountsQueue = new Queue('clientmountsQueue', {
    redis: { host: process.env.REDIS_HOST || 'localhost', port: process.env.REDIS_PORT || 6379 }
});

// Функция обработки задачи
async function processFileJob(job) {
    const { localPath, remotePath, content, sftpConfig } = job.data;
    const sftp = new Client();
    const ssh = new SSHClient();
    const tempRemotePath = `/home/casteradmin/${path.basename(remotePath)}`;

    try {
        // Подключаемся к SFTP
        await sftp.connect({
            ...sftpConfig,
            retries: 3,
            readyTimeout: 10000,
            keepaliveInterval: 10000,
            keepaliveCountMax: 3
        });

        // Загружаем файл в /tmp
        await sftp.put(Buffer.from(content), tempRemotePath);
        await logMessage(LOG_TYPES.I, 'worker', `Uploaded to ${tempRemotePath}, job ID: ${job.id}`);

        // Проверяем целостность
        const stats = await sftp.stat(tempRemotePath);
        if (stats.size !== Buffer.from(content).length) {
            throw new Error(`Incomplete file upload: ${tempRemotePath} size ${stats.size}, expected ${Buffer.from(content).length}`);
        }
        await logMessage(LOG_TYPES.I, 'worker', `Verified file size in /tmp: ${stats.size} bytes`);

        await sftp.end();

        // Задержка для завершения записи
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Подключаемся к SSH
        await new Promise((resolve, reject) => {
            ssh.on('ready', resolve)
                .on('error', reject)
                .connect({
                    ...sftpConfig,
                    keepaliveInterval: 10000,
                    keepaliveCountMax: 3
                });
        });

        // Выполняем sudo cp
        const command = `sudo cp ${tempRemotePath} ${remotePath}`;
        await new Promise((resolve, reject) => {
            ssh.exec(command, { pty: true }, (err, stream) => {
                if (err) reject(err);
                let stderr = '';
                stream.on('close', (code) => {
                    if (code === 0) resolve();
                    else reject(new Error(`sudo cp failed: ${stderr}`));
                }).on('data', (data) => {
                    if (data.toString().includes('password')) {
                        stream.write(`${sftpConfig.password}\n`);
                    }
                }).stderr.on('data', (data) => stderr += data);
            });
        });
        await logMessage(LOG_TYPES.I, 'worker', `Copied to ${remotePath}`);

        await sftp.connect(sftpConfig);
        const remoteStats = await sftp.stat(remotePath);
        if (remoteStats.size !== Buffer.from(content).length) {
            throw new Error(`Incomplete file copy: ${remotePath} size ${remoteStats.size}, expected ${Buffer.from(content).length}`);
        }
        await logMessage(LOG_TYPES.I, 'worker', `Verified copied file size: ${remoteStats.size} bytes`);

        // Удаляем временный файл
        await sftp.delete(tempRemotePath);
        await logMessage(LOG_TYPES.I, 'worker', `Deleted ${tempRemotePath}`);

        return { success: true };
    } catch (error) {
        await logMessage(LOG_TYPES.E, 'worker', `Failed to process job ${job.id} for ${remotePath}: ${error.message}`);
        return { error: error.message };
    } finally {
        try { await sftp.end(); } catch (e) {
            await logMessage(LOG_TYPES.E, 'worker', `Failed to close SFTP: ${e.message}`);
        }
        try { ssh.end(); } catch (e) {
            await logMessage(LOG_TYPES.E, 'worker', `Failed to close SSH: ${e.message}`);
        }
    }
}

// Регистрируем обработчики для каждой очереди
usersQueue.process(async (job) => await processFileJob(job));
groupsQueue.process(async (job) => await processFileJob(job));
mountsQueue.process(async (job) => await processFileJob(job));

// Логирование ошибок очередей
[usersQueue, groupsQueue, mountsQueue].forEach(queue => {
    queue.on('failed', async (job, err) => {
        await logMessage(LOG_TYPES.E, 'worker', `Job ${job.id} in ${queue.name} failed: ${err.message}`);
    });
    queue.on('completed', async (job) => {
        await logMessage(LOG_TYPES.I, 'worker', `Job ${job.id} in ${queue.name} completed`);
    });
});

console.log('Worker started, listening for queue tasks');