
import {ChangeDetectionStrategy, Component, computed, OnInit, signal} from '@angular/core';
import {ScrollingModule} from '@angular/cdk/scrolling';
import {MatIconModule} from '@angular/material/icon';
import {MatButtonModule} from '@angular/material/button';
import { HttpClient } from '@angular/common/http';
import {JsonPipe} from '@angular/common';

@Component({
  selector: 'app-search-catalog',
  imports: [
    ScrollingModule,
    MatIconModule,
    MatButtonModule,
    JsonPipe,
  ],
  templateUrl: './search-catalog.html',
  styleUrl: './search-catalog.scss',
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class SearchCatalog implements  OnInit{
  items = Array.from({length: 50000}).map((_, i) => `Item #${i}`);

  data = signal< fileItem[]>([]);

  constructor(private httpClient: HttpClient) {
  }

  ngOnInit(): void {
   // @ts-ignore

    this.httpClient.get('mb-catalog.json').subscribe(d =>
    {

      console.log("data: ",d);
      // @ts-ignore
      this.data.set(d) ;
    });

  }

  searchQuery = signal<string>('');
  filteredIitems = computed(() => {
    const sq = this.searchQuery();
   // return this.data().filter(x => x.Search.includes(sq));
    const inputs = sq.split(" ");
    return this.data().filter(x => {
      return this.matchAllInputs(x.Search, inputs);
    })
  });

  onSearchUpdated(sq: string) {
    this.searchQuery.set(this.toNormalForm(sq));
  }

  toNormalForm(str : string) {
    return str.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  }

  matchAllInputs (item: string, inputs: string[]) {
    if (inputs.length === 1) {
      return item.indexOf(inputs[0]) > -1;
    }
    let i = inputs.length;
    while (item.indexOf(inputs[i-1]) > -1) {
      i--;
    }
    return i === 0;
  }
}

export interface fileItem {
  Label: string
  Search: string
  Path:string
  Folder:string
}




