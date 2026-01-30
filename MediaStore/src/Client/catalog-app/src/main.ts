import { bootstrapApplication } from '@angular/platform-browser';
import { appConfig } from './app/app.config';
import { App } from './app/app';
import {API_BASE_URL} from './app/openapi/openapi';
import {environment} from './environments/environment';

const providers = [
  {
    provide: API_BASE_URL,
    useValue: environment.mediaStoreApiBase
  },
];

bootstrapApplication(App, {
  providers: [...appConfig.providers, ...providers]
})
  .catch((err) => console.error(err));
