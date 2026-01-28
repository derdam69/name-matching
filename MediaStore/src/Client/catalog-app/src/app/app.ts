import { Component, signal } from '@angular/core';
import { RouterOutlet } from '@angular/router';
import {Search} from './features/catalog/search/search';
import {SearchCatalog} from './features/components/search-catalog/search-catalog';

@Component({
  selector: 'app-root',
  imports: [RouterOutlet, Search, SearchCatalog],
  templateUrl: './app.html',
  styleUrl: './app.css'
})
export class App {
  protected readonly title = signal('catalog-app');
}
