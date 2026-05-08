FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev && npm install -g pm2
COPY . .
EXPOSE 8000
CMD ["pm2-runtime", "main.js"]
