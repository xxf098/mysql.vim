function! ncm2_mysql#on_complete(ctx)
  let l:startcol = a:ctx.startccol
  let l:matches = []
  let current_line = getline('.')
  let words = split(current_line, ' ')
  let count = len(words)
  if count >= 2
    if words[count-1] =~? 'from' || words[count-2] =~? 'from'
      "TODO: support multiple *.mysql files
      let l:matches = get(b:, 'ncm2_mysql_tablenames', [])
      if len(l:matches) == 0 && filereadable('.table_names.data')
        let l:matches = readfile('.table_names.data')
        let b:ncm2_mysql_tablenames = l:matches
      endif
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
