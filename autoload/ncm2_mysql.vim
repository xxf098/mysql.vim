function! ncm2_mysql#on_complete(ctx)
  let l:startcol = a:ctx.startccol
  let l:matches = []
  let current_line = getline('.')
  let words = split(current_line, ' ')
  let count = len(words)
  if count >= 2
    if words[count-1] =~? 'from' || words[count-2] =~? 'from'
      let l:matches = get(b:, 'ncm2_mysql_tablenames', [])
      map(l:matches, "{'word': v:val}")
    endif
  endif
  call ncm2#complete(a:ctx, l:startcol, l:matches)
endfunction

function! ncm2_mysql#init() abort
 call ncm2#register_source({
      \'name': 'mysql',
      \ 'mark': 'mysql',
      \'scope': ['mysql'],
      \ 'priority': 5,
      \ 'on_complete': 'ncm2_mysql#on_complete',
      \ })
endfunction
