"py3file mysql.py
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
  nnoremap <silent><buffer> q :<C-u>bd!<CR>
  silent! normal! ggdG
  call setline('.', split(substitute(output, '[[:return:]]', '', 'g'), '\v\n'))
  silent! normal! zR
  " setlocal nomodifiable
endfunction
"TODO: ftplug
autocmd BufRead,BufNewFile *.mysql nnoremap <buffer><silent> re :call g:DisplaySQLQueryResult()<cr>
