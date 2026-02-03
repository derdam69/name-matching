
import {ChangeDetectionStrategy, Component, computed, OnInit, signal} from '@angular/core';
import {ScrollingModule} from '@angular/cdk/scrolling';
import {MatIconModule} from '@angular/material/icon';
import {MatButtonModule} from '@angular/material/button';
import {HttpClient } from '@angular/common/http';
import {NgOptimizedImage} from '@angular/common';
import {Service} from '../../../openapi/openapi';
import {MatRippleModule, RippleAnimationConfig} from '@angular/material/core';
import { toObservable, toSignal } from '@angular/core/rxjs-interop';
import { debounceTime, switchMap, of, catchError } from 'rxjs';

@Component({
  selector: 'app-search-catalog',
    imports: [
        ScrollingModule,
        MatIconModule,
        MatButtonModule,
        NgOptimizedImage,
        MatRippleModule,
    ],
  providers: [Service],
  templateUrl: './search-catalog.html',
  styleUrl: './search-catalog.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SearchCatalog implements  OnInit{

  data = signal<FileItem[]>([]);

  constructor(private httpClient: HttpClient, private openApi: Service) {
  }

  ngOnInit(): void {
   // this.openApi.open('c:\\temp').subscribe()
    this.httpClient.get('mb-catalog.json').subscribe(d =>
    {
      this.data.set(d as FileItem[]) ;
    });
  }

  isAlertMode = signal(true);
  rippleConfig = computed<RippleAnimationConfig>(() => ({
    enterDuration: this.isAlertMode() ? 100 : 800,
    exitDuration: this.isAlertMode() ? 150 : 700,
  }));

  searchQuery = signal<string>('');

  results = toSignal(
    toObservable(this.searchQuery).pipe(
      debounceTime(300), // Wait for 300ms pause
      switchMap(term => {
        if (!term) return of([]); // Skip API if empty
        return this.openApi.request(term).pipe(
          catchError(() => of([])) // Handle errors
        );
      })))


  filteredIitems = computed(() => {
    const sq = this.searchQuery();
    const tokens = sq.split(" ");
    return this.data().filter(x => {
      return this.matchAllTokens(x.Search, tokens);
    })
  });

  onSearchUpdated(sq: string) {
    this.searchQuery.set(this.toNormalForm(sq));
  }

  toNormalForm(str : string) {
    return str.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  }

  matchAllTokens (item: string, tokens: string[]) {
    if (tokens.length === 1) {
      return item.indexOf(tokens[0]) > -1;
    }
    let i = tokens.length;
    while (item.indexOf(tokens[i-1]) > -1) {
      i--;
    }
    return i === 0;
  }

  openLocation(item: FileItem) {
    this.openApi.open(item.Folder).subscribe(
        {
          error: e => {alert(e)}
        }
    )
  }

  playLocation(item: FileItem) {
    this.openApi.play(item.Folder).subscribe(
        {
          error: e => {alert(e)}
        }
    )
  }

  enqueueLocation(item: FileItem) {
    this.openApi.queue(item.Folder).subscribe(
        {
          error: e => {alert(e)}
        }
    )
  }

  playItem(item: FileItem) {
    this.openApi.play(item.Path).subscribe(
        {
          error: e => {alert(e)}
        }
    )
  }

  enqueueItem(item: FileItem) {
    this.openApi.queue(item.Path).subscribe(
        {
          error: e => {alert(e)}
        }
    )
  }
}

export interface FileItem {
  Label: string
  Search: string
  Path:string
  Folder:string
}




