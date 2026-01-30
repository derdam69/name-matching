
import {ChangeDetectionStrategy, Component, computed, OnInit, signal} from '@angular/core';
import {ScrollingModule} from '@angular/cdk/scrolling';
import {MatIconModule} from '@angular/material/icon';
import {MatButtonModule} from '@angular/material/button';
import {HttpClient } from '@angular/common/http';
import {NgOptimizedImage} from '@angular/common';
import {Service} from '../../../openapi/openapi';

@Component({
  selector: 'app-search-catalog',
    imports: [
        ScrollingModule,
        MatIconModule,
        MatButtonModule,
        NgOptimizedImage
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

  searchQuery = signal<string>('');

  filteredIitems = computed(() => {
    const sq = this.searchQuery();
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

  openFolder(item: FileItem) {

    this.openApi.open(item.Path).subscribe((openApi) => {},
            e => {alert(e)})
  }
}

export interface FileItem {
  Label: string
  Search: string
  Path:string
  Folder:string
}




