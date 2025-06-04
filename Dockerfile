# Используем Node.js 18 на Alpine для минимизации размера
FROM node:18-alpine

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем package.json и устанавливаем зависимости
COPY package.json .
RUN npm install --production

# Копируем исходный код
COPY src/ ./src/

# Копируем .env
COPY .env ./

# Exposing the port
EXPOSE 5473

# Запускаем воркер
CMD ["npm", "start"]