FROM node:26.13.0-slim@sha256:e4db4c914bf8ad241bd24804fc25d272d89bad97e68cdc8a1414862790a02c39

WORKDIR /usr/src/app

COPY package.json package-lock.json ./
RUN npm install --force && mv node_modules ../

COPY . .

CMD ["npm", "run", "dev"]
