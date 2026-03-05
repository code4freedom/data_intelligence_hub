import { useState, useEffect } from 'react'
import { Toaster } from 'react-hot-toast'
import Dashboard from './Dashboard'
import LoginForm from './LoginForm'
import axios from 'axios'

function App() {
  const [token, setToken] = useState<string | null>(localStorage.getItem('vcf_token'))

  useEffect(() => {
    if (token) {
      localStorage.setItem('vcf_token', token)
      axios.defaults.headers.common['Authorization'] = `Bearer ${token}`
    } else {
      localStorage.removeItem('vcf_token')
      delete axios.defaults.headers.common['Authorization']
    }

    const interceptor = axios.interceptors.response.use(
      (response) => response,
      (error) => {
        if (error.response && error.response.status === 401) {
          setToken(null)
        }
        return Promise.reject(error)
      }
    )

    return () => {
      axios.interceptors.response.eject(interceptor)
    }
  }, [token])

  if (!token) {
    return <LoginForm onLogin={setToken} />
  }

  return (
    <div style={{ background: 'white', minHeight: '100vh' }}>
      <Toaster position="bottom-right" />
      <Dashboard />
    </div>
  )
}

export default App
