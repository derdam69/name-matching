import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SearchCatalog } from './search-catalog';

describe('SearchCatalog', () => {
  let component: SearchCatalog;
  let fixture: ComponentFixture<SearchCatalog>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [SearchCatalog]
    })
    .compileComponents();

    fixture = TestBed.createComponent(SearchCatalog);
    component = fixture.componentInstance;
    await fixture.whenStable();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
