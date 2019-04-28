" TODO: sort
" TODO: manage table
" TODO: Table Info
" TODO: jump to foreign key
" TODO: export data
" n jump forward column
" N jump backward column
function! g:JumpToNextColumn(direction)
  let current_line = getline('.')
  let next_col_position = col('.')
  if a:direction == 'backward'
    let next_col_position = strridx(strpart(current_line, 0, next_col_position - 1), '|')
    :echom next_col_position
  endif
  if a:direction == 'forward'
    let next_col_position = stridx(current_line, '|', next_col_position + 1)
    :echom next_col_position
  endif
  if next_col_position == -1
    return
  endif
  let next_col_position += 1
  call cursor(line('.'), next_col_position)
endfunction

function! g:DisplaySQLQueryResult()
  let sql = getline('.')
  let sql = substitute(sql, "`", "\\\\`", "g")
  let cmd = 'python3 ~/.config/nvim/plugged/mysql.vim/mysql.py "' . sql . '"'
  let result = system(cmd)
  let output = result
  let logBufName = "__SQL_Query_Result"
  let bufferNum = bufnr('^' . logBufName)
  if bufferNum == -1
    execute 'new ' . logBufName
    execute 'only'
  else
    execute 'b'. bufferNum
    execute 'f ' . logBufName
  endif
  setlocal modifiable
  setlocal nowrap
  nnoremap <silent><buffer> q :<C-u>bd!<CR>
  nnoremap <silent><buffer> n :call g:JumpToNextColumn('forward')<cr>
  nnoremap <silent><buffer> N :call g:JumpToNextColumn('backward')<cr>
  silent! normal! ggdG
  call setline('.', split(substitute(output, '[[:return:]]', '', 'g'), '\v\n'))
  silent! normal! zR
  " setlocal nomodifiable
endfunction

nnoremap <buffer><silent> re :call g:DisplaySQLQueryResult()<cr>
