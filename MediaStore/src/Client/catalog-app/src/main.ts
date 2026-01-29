import { bootstrapApplication } from '@angular/platform-browser';
import { appConfig } from './app/app.config';
import { App } from './app/app';
import {API_BASE_URL} from './app/openapi/openapi';
import {environment} from './environments/environment';


// Fetches from `http://my-prod-url` in production, `http://my-dev-url` in development.



const providers = [
  // ... other providers, e.g., provideHttpClient()

  {
    provide: API_BASE_URL,
    useValue: environment.mediaStoreApiBase // './proxy-mediastore'
  },
];

bootstrapApplication(App, {
  providers: [...appConfig.providers, ...providers]
})
  .catch((err) => console.error(err));
