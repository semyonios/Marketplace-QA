FROM mcr.microsoft.com/playwright:v1.49.1-noble

WORKDIR /app
RUN npm install --global pnpm@9.15.4
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile
COPY . .
CMD ["pnpm", "test"]
