import { bootstrapApplication } from '@angular/platform-browser';
import { appConfig } from './app/app.config';
import { App } from './app/app';
import {API_BASE_URL} from './app/openapi/openapi';

const providers = [
  // ... other providers, e.g., provideHttpClient()

  {
    provide: API_BASE_URL,
    useValue: './proxy-mediastore'
  },
];

bootstrapApplication(App, {
  providers: [...appConfig.providers, ...providers]
})
  .catch((err) => console.error(err));
