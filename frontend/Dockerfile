FROM node:26.11.1-slim@sha256:193fe51b64e77981119c98c2002c9e32a70e2f006fb4d25068ce0558998917f0

WORKDIR /usr/src/app

COPY package.json package-lock.json ./
RUN npm install --force && mv node_modules ../

COPY . .

CMD ["npm", "run", "dev"]
