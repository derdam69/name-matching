
import {ChangeDetectionStrategy, Component} from '@angular/core';
import {ScrollingModule} from '@angular/cdk/scrolling';

@Component({
  selector: 'app-search',
  templateUrl: './search.html',
  styleUrl: './search.css',
  changeDetection: ChangeDetectionStrategy.Default,
  imports: [ScrollingModule],
})
export class Search {
  items = Array.from({length:30000}).map((_, i) => `Item #${i}`);
}
