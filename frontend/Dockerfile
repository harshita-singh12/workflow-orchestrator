FROM node:20-alpine AS build
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
# Vite only exposes VITE_-prefixed vars to client code, and only at build time — this bakes
# the dev API key into the bundle so the built dashboard can authenticate against the server.
# See docker-compose.yml's `frontend.build.args`.
ARG VITE_API_KEY=dev-local-key-change-me
ENV VITE_API_KEY=$VITE_API_KEY
RUN npm run build

FROM nginx:1.27-alpine
COPY --from=build /src/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
