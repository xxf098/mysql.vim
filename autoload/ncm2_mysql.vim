function! ncm2_mysql#on_complete(ctx)
  let l:startcol = a:ctx.startccol
  let l:matches = get(b:, 'ncm2_mysql_tablenames', [])
  map(l:matches, "{'word': v:val}")
  call ncm2#complete(a:ctx, l:startcol, l:matches)
endfunction

function! ncm2_mysql#init() abort
 call ncm2#register_source({
      \'name': 'ncm2_mysql',
      \ 'mark': 'ncm2_mysql',
      \'scope': ['mysql'],
      \ 'priority': 5,
      \ 'on_complete': 'ncm2_mysql#on_complete',
      \ })
endfunction
